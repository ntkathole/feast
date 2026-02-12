import functools
import logging
import re
import sys
import textwrap
from abc import ABC
from typing import Any, Callable, Dict, Optional, Union

import dill

from feast.protos.feast.core.Transformation_pb2 import (
    SubstraitTransformationV2 as SubstraitTransformationProto,
)
from feast.protos.feast.core.Transformation_pb2 import (
    UserDefinedFunctionV2 as UserDefinedFunctionProto,
)
from feast.transformation.factory import (
    TRANSFORMATION_CLASS_FOR_TYPE,
    get_transformation_class_from_type,
)
from feast.transformation.mode import TransformationMode

logger = logging.getLogger(__name__)


def _recompile_udf_from_source(
    body_text: str, func_name: Optional[str] = None
) -> Callable:
    """
    Recompile a UDF from its source text. This is used as a fallback when
    dill-deserialized bytecode is incompatible with the current Python version
    (e.g., serialized with Python 3.11 but loaded on Python 3.12).
    """
    # Extract the function definition by stripping the decorator
    lines = body_text.split("\n")
    func_start = None
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("def "):
            func_start = i
            break

    if func_start is None:
        raise ValueError("Could not find function definition in UDF source text")

    func_source = "\n".join(lines[func_start:])
    func_source = textwrap.dedent(func_source)

    # Provide common imports that UDFs typically use
    import numpy as np
    import pandas as pd

    exec_globals: Dict[str, Any] = {
        "__builtins__": __builtins__,
        "pd": pd,
        "pandas": pd,
        "np": np,
        "numpy": np,
    }

    exec(func_source, exec_globals)

    # Find the compiled function
    if func_name and func_name in exec_globals:
        func = exec_globals[func_name]
        func.__module__ = "__main__"
        return func

    # Search for the first function defined in the source
    # Parse the source to find the function name
    match = re.match(r"def\s+(\w+)\s*\(", func_source)
    if match:
        extracted_name = match.group(1)
        if extracted_name in exec_globals:
            func = exec_globals[extracted_name]
            func.__module__ = "__main__"
            return func

    raise ValueError(f"Could not extract function '{func_name}' from UDF source text")


def safe_load_udf(
    body: bytes,
    body_text: str,
    func_name: Optional[str] = None,
) -> Callable:
    """
    Safely load a UDF, preferring recompilation from source text over
    dill-deserialized bytecode.

    Python bytecode is NOT portable across Python minor versions (e.g.,
    3.11 vs 3.12). When a UDF is serialized via ``dill`` on one Python
    version and deserialized on another, executing the incompatible
    bytecode causes a **segfault** (SIGSEGV / exit code 139).

    Since ``dill`` cannot reliably detect this version mismatch, we
    always try to recompile the UDF from its source text first. Source
    text is compiled by the current Python interpreter, guaranteeing
    bytecode compatibility. We only fall back to ``dill.loads`` when
    source recompilation fails (e.g., if the UDF has complex
    dependencies that aren't available in the exec namespace).

    Args:
        body: The dill-serialized UDF bytes.
        body_text: The source code text of the UDF (including decorator).
        func_name: Optional name of the function to extract.

    Returns:
        The callable UDF function.
    """
    # Strategy: always try source recompilation first for safety,
    # fall back to dill deserialization if source compilation fails.
    if body_text:
        try:
            udf = _recompile_udf_from_source(body_text, func_name)
            logger.debug(
                "Successfully recompiled UDF '%s' from source text for Python %d.%d.",
                func_name or "<unknown>",
                sys.version_info.major,
                sys.version_info.minor,
            )
            return udf
        except Exception as e:
            logger.warning(
                "Failed to recompile UDF '%s' from source text (%s). "
                "Falling back to dill deserialization.",
                func_name or "<unknown>",
                e,
            )

    # Fallback: use dill deserialization (may segfault if Python version differs)
    try:
        return dill.loads(body)
    except Exception as e:
        raise RuntimeError(
            f"Failed to load UDF '{func_name}': both source recompilation and "
            f"dill deserialization failed. Dill error: {e}"
        ) from e


class Transformation(ABC):
    """
    Base Transformation class. Can be used to define transformations that can be applied to FeatureViews.
    Also encapsulates the logic to serialize and deserialize the transformation to and from proto. This is
    important for the future transformation lifecycle management.
    E.g.:
    pandas_transformation = Transformation(
        mode=TransformationMode.PANDAS,
        udf=lambda df: df.assign(new_column=df['column1'] + df['column2']),
    )
    """

    udf: Callable[[Any], Any]
    udf_string: str

    def __new__(
        cls,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        udf_string: str,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        *args,
        **kwargs,
    ) -> "Transformation":
        """
        Creates a Transformation object.
        Args:
            mode: (required) The mode of the transformation. Choose one from TransformationMode.
            udf: (required) The user-defined transformation function.
            udf_string: (required) The string representation of the udf. The dill get source doesn't
            work for all cases when extracting the source code from the udf. So it's better to pass
            the source code as a string.
            name: (optional) The name of the transformation.
            tags: (optional) Metadata tags for the transformation.
            description: (optional) A description of the transformation.
            owner: (optional) The owner of the transformation.
        """
        if cls is Transformation:
            if isinstance(mode, TransformationMode):
                mode = mode.value

            if mode.lower() in TRANSFORMATION_CLASS_FOR_TYPE:
                subclass = get_transformation_class_from_type(mode.lower())
                return super().__new__(subclass)

            raise ValueError(
                f"Invalid mode: {mode}. Choose one from TransformationMode."
            )

        return super().__new__(cls)

    def __init__(
        self,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        udf_string: str,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
    ):
        self.mode = mode
        self.udf = udf
        self.udf_string = udf_string
        self.name = name or udf.__name__
        self.tags = tags or {}
        self.description = description
        self.owner = owner

    def to_proto(self) -> Union[UserDefinedFunctionProto, SubstraitTransformationProto]:
        mode_str = (
            self.mode.value if isinstance(self.mode, TransformationMode) else self.mode
        )
        return UserDefinedFunctionProto(
            name=self.udf.__name__,
            body=dill.dumps(self.udf, recurse=True),
            body_text=self.udf_string,
            mode=mode_str,
        )

    def __deepcopy__(self, memo: Optional[Dict[int, Any]] = None) -> "Transformation":
        return Transformation(mode=self.mode, udf=self.udf, udf_string=self.udf_string)

    def transform(self, *inputs: Any) -> Any:
        raise NotImplementedError

    def transform_arrow(self, *args, **kwargs) -> Any:
        pass

    def transform_singleton(self, *args, **kwargs) -> Any:
        pass

    def infer_features(self, *args, **kwargs) -> Any:
        raise NotImplementedError


def transformation(
    mode: Union[TransformationMode, str],
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    description: Optional[str] = "",
    owner: Optional[str] = "",
):
    def mainify(obj):
        # Needed to allow dill to properly serialize the udf. Otherwise, clients will need to have a file with the same
        # name as the original file defining the sfv.
        if obj.__module__ != "__main__":
            obj.__module__ = "__main__"

    def decorator(user_function):
        udf_string = dill.source.getsource(user_function)
        mainify(user_function)
        transformation_obj = Transformation(
            mode=mode,
            name=name or user_function.__name__,
            tags=tags,
            description=description,
            owner=owner,
            udf=user_function,
            udf_string=udf_string,
        )
        functools.update_wrapper(wrapper=transformation_obj, wrapped=user_function)
        return transformation_obj

    return decorator
