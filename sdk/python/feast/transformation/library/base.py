# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Base classes for Feast transformations.
"""

import copy
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd


class FitMixin(ABC):
    """Mixin class for transformers that need to be fitted on training data."""

    def __init__(self) -> None:
        self._is_fitted = False
        self._fitted_params: Dict[str, Any] = {}

    @abstractmethod
    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "FitMixin":
        """
        Fit the transformer on the training data.

        Args:
            X: Training data
            y: Target values (optional, for supervised transformations)

        Returns:
            self
        """
        pass

    @property
    def is_fitted(self) -> bool:
        """Check if the transformer has been fitted."""
        return self._is_fitted

    def _check_is_fitted(self):
        """Raise an error if the transformer is not fitted."""
        if not self._is_fitted:
            raise ValueError(
                f"{self.__class__.__name__} has not been fitted yet. Call fit() first."
            )


class TransformMixin(ABC):
    """Mixin class for transformers that can transform data."""

    @abstractmethod
    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame, List[Any]]:
        """
        Transform the input data.

        Args:
            X: Input data to transform

        Returns:
            Transformed data
        """
        pass

    def fit_transform(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> Union[np.ndarray, pd.DataFrame, List[Any]]:
        """
        Fit the transformer and transform the data in one step.

        Args:
            X: Training data
            y: Target values (optional)

        Returns:
            Transformed data
        """
        if hasattr(self, "fit"):
            self.fit(X, y)
        return self.transform(X)


class BaseTransformer(FitMixin, TransformMixin, ABC):
    """
    Base class for all Feast transformations.

    This class provides the common interface and functionality for all transformations
    in the Feast transformations library.
    """

    def __init__(self, name: Optional[str] = None, **kwargs):
        """
        Initialize the base transformer.

        Args:
            name: Optional name for the transformer
            **kwargs: Additional parameters
        """
        super().__init__()
        self.name = name or self.__class__.__name__
        self.params = kwargs

    def get_params(self) -> Dict[str, Any]:
        """Get the parameters of the transformer."""
        return copy.deepcopy(self.params)

    def set_params(self, **params) -> "BaseTransformer":
        """Set the parameters of the transformer."""
        for key, value in params.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.params[key] = value
        return self

    def __repr__(self) -> str:
        """String representation of the transformer."""
        params_str = ", ".join([f"{k}={v}" for k, v in self.params.items()])
        return f"{self.__class__.__name__}({params_str})"

    def _validate_input(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """
        Validate and convert input to a standard format.

        Args:
            X: Input data

        Returns:
            Validated data in numpy array or pandas DataFrame format
        """
        if isinstance(X, list):
            if len(X) > 0 and isinstance(X[0], (int, float)):
                return np.array(X)
            else:
                return pd.DataFrame(X)
        elif isinstance(X, pd.Series):
            return pd.DataFrame(X)
        else:
            return X

    def _ensure_2d(
        self, X: Union[np.ndarray, pd.DataFrame]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """
        Ensure input is 2D.

        Args:
            X: Input data

        Returns:
            2D data
        """
        if isinstance(X, np.ndarray) and X.ndim == 1:
            return X.reshape(-1, 1)
        return X


class FeastTransformer(BaseTransformer):
    """
    Base class for transformations that integrate with Feast's transformation framework.

    This class provides additional functionality for transformations that need to work
    with Feast's OnDemandFeatureView and other Feast components.
    """

    def __init__(self, name: Optional[str] = None, **kwargs):
        super().__init__(name, **kwargs)
        self._feast_compatible = True

    def to_feast_udf(
        self, input_columns: List[str], output_columns: Optional[List[str]] = None
    ) -> Callable[[pd.DataFrame], pd.DataFrame]:
        """
        Convert the transformer to a Feast-compatible UDF.

        Args:
            input_columns: List of input column names
            output_columns: List of output column names (optional)

        Returns:
            Feast-compatible UDF function
        """

        def feast_udf(df: pd.DataFrame) -> pd.DataFrame:
            # Extract input columns
            X = df[input_columns] if len(input_columns) > 1 else df[input_columns[0]]

            # Transform
            transformed = self.transform(X)

            # Create output DataFrame
            nonlocal output_columns
            if output_columns is None:
                if isinstance(transformed, np.ndarray):
                    if transformed.ndim == 1:
                        output_columns = [f"{self.name}_output"]
                    else:
                        output_columns = [
                            f"{self.name}_output_{i}"
                            for i in range(transformed.shape[1])
                        ]
                elif hasattr(transformed, "columns"):
                    output_columns = transformed.columns.tolist()
                else:
                    output_columns = [f"{self.name}_output"]

            if isinstance(transformed, np.ndarray):
                if transformed.ndim == 1:
                    result_df = pd.DataFrame({output_columns[0]: transformed})
                else:
                    result_df = pd.DataFrame(transformed, columns=output_columns)
            else:
                result_df = transformed.copy()
                result_df.columns = output_columns

            return result_df

        return feast_udf
