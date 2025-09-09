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
Scaling and normalization transformations for Feast.
"""

from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd

from feast.transformation.library.base import FeastTransformer


class MinMaxScaler(FeastTransformer):
    """
    Transform features by scaling each feature to a given range.

    This estimator scales and translates each feature individually such that it is in
    the given range on the training set, e.g. between zero and one.

    Parameters:
        feature_range: tuple (min, max), default=(0, 1)
            Desired range of transformed data.
        clip: bool, default=False
            Set to True to clip transformed values of held-out data to provided feature range.
    """

    def __init__(self, feature_range: tuple = (0, 1), clip: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.feature_range = feature_range
        self.clip = clip
        self._data_min: Optional[np.ndarray] = None
        self._data_max: Optional[np.ndarray] = None
        self._scale: Optional[np.ndarray] = None
        self._min: Optional[np.ndarray] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "MinMaxScaler":
        """Compute the minimum and maximum to be used for later scaling."""
        X = self._validate_input(X)
        X = self._ensure_2d(X)

        if isinstance(X, pd.DataFrame):
            self._data_min = X.min().values
            self._data_max = X.max().values
        else:
            self._data_min = np.min(X, axis=0)
            self._data_max = np.max(X, axis=0)

        # Handle constant features
        feature_range = self.feature_range
        if self._data_max is not None and self._data_min is not None:
            data_range = self._data_max - self._data_min
            data_range[data_range == 0] = 1.0

            self._scale = (feature_range[1] - feature_range[0]) / data_range
            self._min = feature_range[0] - self._data_min * self._scale

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """Scale features of X according to feature_range."""
        self._check_is_fitted()

        X = self._validate_input(X)
        X = self._ensure_2d(X)

        if isinstance(X, pd.DataFrame):
            X_scaled = X.values.copy()
        else:
            X_scaled = X.copy()

        if self._scale is not None and self._min is not None:
            X_scaled = X_scaled * self._scale + self._min

        if self.clip:
            X_scaled = np.clip(X_scaled, self.feature_range[0], self.feature_range[1])

        if isinstance(X, pd.DataFrame):
            return pd.DataFrame(X_scaled, columns=X.columns, index=X.index)
        else:
            return X_scaled


class StandardScaler(FeastTransformer):
    """
    Standardize features by removing the mean and scaling to unit variance.

    The standard score of a sample x is calculated as:
        z = (x - u) / s
    where u is the mean of the training samples and s is the standard deviation.

    Parameters:
        with_mean: bool, default=True
            If True, center the data before scaling.
        with_std: bool, default=True
            If True, scale the data to unit variance.
    """

    def __init__(self, with_mean: bool = True, with_std: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.with_mean = with_mean
        self.with_std = with_std
        self._mean: Optional[np.ndarray] = None
        self._scale: Optional[np.ndarray] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "StandardScaler":
        """Compute the mean and std to be used for later scaling."""
        X = self._validate_input(X)
        X = self._ensure_2d(X)

        if isinstance(X, pd.DataFrame):
            if self.with_mean:
                self._mean = X.mean().values
            else:
                self._mean = np.zeros(X.shape[1])

            if self.with_std:
                self._scale = X.std().values
                # Handle constant features
                if self._scale is not None:
                    self._scale[self._scale == 0] = 1.0
            else:
                self._scale = np.ones(X.shape[1])
        else:
            if self.with_mean:
                self._mean = np.mean(X, axis=0)
            else:
                self._mean = np.zeros(X.shape[1])

            if self.with_std:
                self._scale = np.std(X, axis=0)
                # Handle constant features
                if self._scale is not None:
                    self._scale[self._scale == 0] = 1.0
            else:
                self._scale = np.ones(X.shape[1])

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """Perform standardization by centering and scaling."""
        self._check_is_fitted()

        X = self._validate_input(X)
        X = self._ensure_2d(X)

        if isinstance(X, pd.DataFrame):
            X_scaled = X.values.copy()
        else:
            X_scaled = X.copy()

        if self.with_mean and self._mean is not None:
            X_scaled = X_scaled - self._mean

        if self.with_std and self._scale is not None:
            X_scaled = X_scaled / self._scale

        if isinstance(X, pd.DataFrame):
            return pd.DataFrame(X_scaled, columns=X.columns, index=X.index)
        else:
            return X_scaled


class RobustScaler(FeastTransformer):
    """
    Scale features using statistics that are robust to outliers.

    This Scaler removes the median and scales the data according to the quantile range
    (defaults to IQR: Interquartile Range). The IQR is the range between the 1st quartile
    (25th quantile) and the 3rd quartile (75th quantile).
    Parameters:
        with_centering: bool, default=True
            If True, center the data before scaling.
        with_scaling: bool, default=True
            If True, scale the data to interquartile range.
        quantile_range: tuple (q_min, q_max), default=(25.0, 75.0)
            Quantile range used to calculate scale_.
    """

    def __init__(
        self,
        with_centering: bool = True,
        with_scaling: bool = True,
        quantile_range: tuple = (25.0, 75.0),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.with_centering = with_centering
        self.with_scaling = with_scaling
        self.quantile_range = quantile_range
        self._center: Optional[np.ndarray] = None
        self._scale: Optional[np.ndarray] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "RobustScaler":
        """Compute the median and quantiles to be used for later scaling."""
        X = self._validate_input(X)
        X = self._ensure_2d(X)

        if isinstance(X, pd.DataFrame):
            if self.with_centering:
                self._center = X.median().values
            else:
                self._center = np.zeros(X.shape[1])

            if self.with_scaling:
                q_min, q_max = self.quantile_range
                quantiles = X.quantile([q_min / 100.0, q_max / 100.0]).values
                self._scale = quantiles[1] - quantiles[0]
                # Handle constant features
                if self._scale is not None:
                    self._scale[self._scale == 0] = 1.0
            else:
                self._scale = np.ones(X.shape[1])
        else:
            if self.with_centering:
                self._center = np.median(X, axis=0)
            else:
                self._center = np.zeros(X.shape[1])

            if self.with_scaling:
                q_min, q_max = self.quantile_range
                quantiles = np.percentile(X, [q_min, q_max], axis=0)
                self._scale = quantiles[1] - quantiles[0]
                # Handle constant features
                if self._scale is not None:
                    self._scale[self._scale == 0] = 1.0
            else:
                self._scale = np.ones(X.shape[1])

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """Center and scale the data."""
        self._check_is_fitted()

        X = self._validate_input(X)
        X = self._ensure_2d(X)

        if isinstance(X, pd.DataFrame):
            X_scaled = X.values.copy()
        else:
            X_scaled = X.copy()

        if self.with_centering and self._center is not None:
            X_scaled = X_scaled - self._center

        if self.with_scaling and self._scale is not None:
            X_scaled = X_scaled / self._scale

        if isinstance(X, pd.DataFrame):
            return pd.DataFrame(X_scaled, columns=X.columns, index=X.index)
        else:
            return X_scaled


class Normalizer(FeastTransformer):
    """
    Normalize samples individually to unit norm.

    Each sample (i.e. each row of the data matrix) with at least one non zero component
    is rescaled independently of other samples so that its norm (l1, l2 or inf) equals one.

    Parameters:
        norm: str, default='l2'
            The norm to use to normalize each non zero sample. Can be 'l1', 'l2', or 'max'.
    """

    def __init__(self, norm: str = "l2", **kwargs):
        super().__init__(**kwargs)
        if norm not in ["l1", "l2", "max"]:
            raise ValueError("norm must be 'l1', 'l2', or 'max'")
        self.norm = norm

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "Normalizer":
        """Fit does nothing and is only present for API consistency."""
        X = self._validate_input(X)
        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """Scale each non zero row of X to unit norm."""
        self._check_is_fitted()

        X = self._validate_input(X)
        X = self._ensure_2d(X)

        if isinstance(X, pd.DataFrame):
            X_norm = X.values.copy()
        else:
            X_norm = X.copy()

        if self.norm == "l1":
            norms = np.sum(np.abs(X_norm), axis=1)
        elif self.norm == "l2":
            norms = np.sqrt(np.sum(X_norm**2, axis=1))
        elif self.norm == "max":
            norms = np.max(np.abs(X_norm), axis=1)

        # Avoid division by zero
        norms[norms == 0] = 1.0

        X_norm = X_norm / norms[:, np.newaxis]

        if isinstance(X, pd.DataFrame):
            return pd.DataFrame(X_norm, columns=X.columns, index=X.index)
        else:
            return X_norm
