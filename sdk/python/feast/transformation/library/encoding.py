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
Encoding transformations for Feast.
"""

from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from feast.transformation.library.base import FeastTransformer


class OneHotEncoder(FeastTransformer):
    """
    Encode categorical features as a one-hot numeric array.

    Parameters:
        categories: 'auto' or list of array-like, default='auto'
            Categories (unique values) per feature.
        drop: 'first', 'if_binary' or None, default=None
            Specifies a methodology to use to drop one of the categories per feature.
        sparse: bool, default=False
            Will return sparse matrix if set True else will return an array.
        handle_unknown: str, default='error'
            Whether to raise an error or ignore if an unknown categorical feature is present.
    """

    def __init__(
        self,
        categories: Union[str, List[List[str]]] = "auto",
        drop: Optional[str] = None,
        sparse: bool = False,
        handle_unknown: str = "error",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.categories = categories
        self.drop = drop
        self.sparse = sparse
        self.handle_unknown = handle_unknown
        self._categories: Optional[List[List[Any]]] = None
        self._drop_idx_: Optional[List[Optional[int]]] = None
        self._n_features_: Optional[int] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "OneHotEncoder":
        """Fit OneHotEncoder to X."""
        X = self._validate_input(X)

        if isinstance(X, pd.DataFrame):
            self._n_features_ = X.shape[1]
        else:
            self._n_features_ = X.shape[1] if X.ndim > 1 else 1

        self._categories = []
        self._drop_idx_ = []

        for i in range(self._n_features_ or 0):
            if isinstance(X, pd.DataFrame):
                feature_data = X.iloc[:, i]
            else:
                feature_data = X[:, i] if X.ndim > 1 else X

            # Get unique categories
            if self.categories == "auto":
                categories = sorted(pd.Series(feature_data).dropna().unique())
            else:
                categories = (
                    self.categories[i] if isinstance(self.categories, list) else []
                )

            self._categories.append(categories)

            # Handle dropping
            if self.drop == "first":
                self._drop_idx_.append(0)
            elif self.drop == "if_binary" and len(categories) == 2:
                self._drop_idx_.append(0)
            else:
                self._drop_idx_.append(None)

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """Transform X using one-hot encoding."""
        self._check_is_fitted()

        X = self._validate_input(X)

        if isinstance(X, pd.DataFrame):
            feature_names = X.columns.tolist()
        else:
            feature_names = [f"feature_{i}" for i in range(self._n_features_ or 0)]

        encoded_features = []
        encoded_names = []

        for i in range(self._n_features_ or 0):
            if isinstance(X, pd.DataFrame):
                feature_data = X.iloc[:, i]
            else:
                feature_data = X[:, i] if X.ndim > 1 else X

            categories = (self._categories or [])[i]
            drop_idx = (self._drop_idx_ or [])[i]

            # Create one-hot encoding
            for j, category in enumerate(categories):
                if drop_idx is not None and j == drop_idx:
                    continue

                encoded_feature = (feature_data == category).astype(int)
                encoded_features.append(encoded_feature)
                encoded_names.append(f"{feature_names[i]}_{category}")

        if encoded_features:
            result = np.column_stack(encoded_features)
            if isinstance(X, pd.DataFrame):
                return pd.DataFrame(result, columns=encoded_names, index=X.index)
            else:
                return result
        else:
            # Handle case where all features are dropped
            if isinstance(X, pd.DataFrame):
                return pd.DataFrame(index=X.index)
            else:
                return np.empty((X.shape[0], 0))


class ImpactEncoder(FeastTransformer):
    """
    Encode categorical features using target encoding (impact encoding).

    This encoder replaces categorical values with the mean of the target variable
    for each category, with optional smoothing to handle rare categories.

    Parameters:
        smoothing: float, default=1.0
            Smoothing factor to balance between category mean and global mean.
        min_samples_leaf: int, default=1
            Minimum number of samples in a leaf for a category to be considered.
        handle_unknown: str, default='value'
            How to handle unknown categories. 'value' uses the global mean.
    """

    def __init__(
        self,
        smoothing: float = 1.0,
        min_samples_leaf: int = 1,
        handle_unknown: str = "value",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.smoothing = smoothing
        self.min_samples_leaf = min_samples_leaf
        self.handle_unknown = handle_unknown
        self._category_means: Optional[List[Dict[Any, float]]] = None
        self._global_mean: Optional[float] = None
        self._n_features_: Optional[int] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "ImpactEncoder":
        """Fit ImpactEncoder to X and y."""
        if y is None:
            raise ValueError("ImpactEncoder requires target values y")

        X = self._validate_input(X)
        y = self._validate_input(y)

        if isinstance(X, pd.DataFrame):
            self._n_features_ = X.shape[1]
        else:
            self._n_features_ = X.shape[1] if X.ndim > 1 else 1

        self._global_mean = np.mean(y)
        self._category_means = []

        for i in range(self._n_features_ or 0):
            if isinstance(X, pd.DataFrame):
                feature_data = X.iloc[:, i]
            else:
                feature_data = X[:, i] if X.ndim > 1 else X

            category_means = {}
            for category in pd.Series(feature_data).dropna().unique():
                mask = feature_data == category
                if np.sum(mask) >= self.min_samples_leaf:
                    category_mean = np.mean(y[mask])
                    # Apply smoothing
                    n_samples = np.sum(mask)
                    smoothed_mean = (
                        category_mean * n_samples
                        + (self._global_mean or 0) * self.smoothing
                    ) / (n_samples + self.smoothing)
                    category_means[category] = smoothed_mean
                else:
                    category_means[category] = self._global_mean or 0

            self._category_means.append(category_means)

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """Transform X using impact encoding."""
        self._check_is_fitted()

        X = self._validate_input(X)

        if isinstance(X, pd.DataFrame):
            result = X.copy()
        else:
            # Create a numeric array for the result
            result = np.zeros_like(X, dtype=float)

        for i in range(self._n_features_ or 0):
            if isinstance(X, pd.DataFrame):
                feature_data = result.iloc[:, i]
            else:
                feature_data = result[:, i] if result.ndim > 1 else result

            category_means = (self._category_means or [])[i]
            encoded_values: List[float] = []

            for value in feature_data:
                if pd.isna(value):
                    encoded_values.append(self._global_mean or 0)
                elif value in category_means:
                    encoded_values.append(category_means[value])
                else:
                    # Handle unknown categories
                    if self.handle_unknown == "value":
                        encoded_values.append(self._global_mean or 0)
                    else:
                        raise ValueError(f"Unknown category {value} found")

            if isinstance(X, pd.DataFrame):
                result.iloc[:, i] = encoded_values
            else:
                result[:, i] = encoded_values

        if isinstance(X, pd.DataFrame):
            return result
        else:
            return result


class LabelEncoder(FeastTransformer):
    """
    Encode target labels with value between 0 and n_classes-1.

    This transformer should be used to encode target values, i.e. y, and not the input X.

    Parameters:
        classes_: array-like of shape (n_classes,)
            Holds the label for each class.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._classes: Optional[List[Any]] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "LabelEncoder":
        """Fit label encoder."""
        if y is not None:
            # If y is provided, use it for encoding
            data = y
        else:
            # Otherwise use X
            data = X

        data = self._validate_input(data)

        if isinstance(data, pd.DataFrame):
            data = data.iloc[:, 0]  # Use first column
        elif isinstance(data, np.ndarray) and data.ndim > 1:
            data = data.flatten()  # Flatten multi-dimensional arrays

        self._classes = sorted(pd.Series(data).dropna().unique())
        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.Series]:
        """Transform labels to normalized encoding."""
        self._check_is_fitted()

        X = self._validate_input(X)

        if isinstance(X, pd.DataFrame):
            data = X.iloc[:, 0]  # Use first column
        else:
            if X.ndim > 1:
                data = X.flatten()  # Flatten multi-dimensional arrays
            else:
                data = X

        # Create mapping from class to index
        class_to_index = {cls: idx for idx, cls in enumerate(self._classes or [])}

        # Transform data
        if isinstance(data, pd.Series):
            result = data.map(class_to_index)
            # Handle unknown values
            result = result.fillna(-1)
        else:
            result = np.array([class_to_index.get(x, -1) for x in data])

        return result

    def inverse_transform(
        self, X: Union[np.ndarray, pd.Series]
    ) -> Union[np.ndarray, pd.Series]:
        """Transform labels back to original encoding."""
        self._check_is_fitted()

        classes = self._classes or []
        if isinstance(X, pd.Series):
            result = X.map({idx: cls for idx, cls in enumerate(classes)})
            # Handle unknown values
            result = result.fillna("unknown")
        else:
            result = np.array(
                [classes[idx] if 0 <= idx < len(classes) else "unknown" for idx in X]
            )

        return result
