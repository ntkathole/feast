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
Feast Transformation Library

This module provides a comprehensive library of transformation functions and classes for Feast,
including scaling, encoding, text processing, and other common ML transformations.
"""

from feast.transformation.library.base import BaseTransformer, FitMixin, TransformMixin
from feast.transformation.library.encoding import (
    ImpactEncoder,
    LabelEncoder,
    OneHotEncoder,
)
from feast.transformation.library.scaling import (
    MinMaxScaler,
    Normalizer,
    RobustScaler,
    StandardScaler,
)
from feast.transformation.library.text import (
    TextChunker,
    TextEmbedder,
    TextPreprocessor,
)

__all__ = [
    # Base classes
    "BaseTransformer",
    "FitMixin",
    "TransformMixin",
    # Scaling transformations
    "MinMaxScaler",
    "StandardScaler",
    "RobustScaler",
    "Normalizer",
    # Encoding transformations
    "OneHotEncoder",
    "ImpactEncoder",
    "LabelEncoder",
    # Text transformations
    "TextEmbedder",
    "TextChunker",
    "TextPreprocessor",
]
