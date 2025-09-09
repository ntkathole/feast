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
Text processing transformations for Feast.
"""

import re
from typing import Any, Callable, List, Optional, Union

import numpy as np
import pandas as pd

from feast.transformation.library.base import FeastTransformer


class TextPreprocessor(FeastTransformer):
    """
    Text preprocessing transformer for cleaning and normalizing text data.

    Parameters:
        lowercase: bool, default=True
            Convert text to lowercase.
        remove_punctuation: bool, default=True
            Remove punctuation marks.
        remove_numbers: bool, default=False
            Remove numeric characters.
        remove_extra_whitespace: bool, default=True
            Remove extra whitespace and normalize spacing.
        remove_stopwords: bool, default=False
            Remove common stopwords (requires nltk).
        custom_stopwords: list, default=None
            Custom list of stopwords to remove.
    """

    def __init__(
        self,
        lowercase: bool = True,
        remove_punctuation: bool = True,
        remove_numbers: bool = False,
        remove_extra_whitespace: bool = True,
        remove_stopwords: bool = False,
        custom_stopwords: Optional[List[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.lowercase = lowercase
        self.remove_punctuation = remove_punctuation
        self.remove_numbers = remove_numbers
        self.remove_extra_whitespace = remove_extra_whitespace
        self.remove_stopwords = remove_stopwords
        self.custom_stopwords = custom_stopwords or []
        self._stopwords: Optional[set] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "TextPreprocessor":
        """Fit the text preprocessor."""
        if self.remove_stopwords:
            try:
                import nltk
                from nltk.corpus import stopwords

                nltk.download("stopwords", quiet=True)
                self._stopwords = set(stopwords.words("english"))
            except ImportError:
                print("Warning: nltk not available, stopwords removal disabled")
                self.remove_stopwords = False

        if self.custom_stopwords:
            if self._stopwords is None:
                self._stopwords = set()
            self._stopwords.update(self.custom_stopwords)

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame, List[Any]]:
        """Preprocess text data."""
        self._check_is_fitted()

        # For text processing, keep lists as lists
        if isinstance(X, list):
            list_result = [self._preprocess_text(str(x)) for x in X]
            return np.array(list_result)

        X = self._validate_input(X)

        if isinstance(X, pd.DataFrame):
            df_result = X.copy()
            for col in X.columns:
                df_result[col] = df_result[col].astype(str).apply(self._preprocess_text)
            return df_result
        else:
            if X.ndim == 1:
                array_1d_result: List[str] = [self._preprocess_text(str(x)) for x in X]
                return array_1d_result
            else:
                array_2d_result: List[List[str]] = [
                    [self._preprocess_text(str(x)) for x in row] for row in X
                ]
                return array_2d_result

    def _preprocess_text(self, text: str) -> str:
        """Preprocess a single text string."""
        if not isinstance(text, str):
            text = str(text)

        # Convert to lowercase
        if self.lowercase:
            text = text.lower()

        # Remove punctuation
        if self.remove_punctuation:
            text = re.sub(r"[^\w\s]", "", text)

        # Remove numbers
        if self.remove_numbers:
            text = re.sub(r"\d+", "", text)

        # Remove extra whitespace
        if self.remove_extra_whitespace:
            text = re.sub(r"\s+", " ", text).strip()

        # Remove stopwords
        if self.remove_stopwords and self._stopwords:
            words = text.split()
            words = [word for word in words if word not in self._stopwords]
            text = " ".join(words)

        return text


class TextChunker(FeastTransformer):
    """
    Text chunking transformer for splitting long texts into smaller chunks.

    Parameters:
        chunk_size: int, default=512
            Maximum number of tokens per chunk.
        chunk_overlap: int, default=50
            Number of tokens to overlap between chunks.
        strategy: str, default='fixed'
            Chunking strategy: 'fixed', 'semantic', 'sentence'.
        tokenizer: str, default='simple'
            Tokenization method: 'simple', 'nltk', 'spacy'.
    """

    def __init__(
        self,
        chunk_size: int = 512,
        chunk_overlap: int = 50,
        strategy: str = "fixed",
        tokenizer: str = "simple",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.strategy = strategy
        self.tokenizer = tokenizer
        self._tokenizer_func: Optional[Callable[[str], List[str]]] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "TextChunker":
        """Fit the text chunker."""
        if self.tokenizer == "nltk":
            try:
                import nltk

                nltk.download("punkt", quiet=True)
                from nltk.tokenize import word_tokenize

                self._tokenizer_func = word_tokenize
            except ImportError:
                print("Warning: nltk not available, using simple tokenizer")
                self._tokenizer_func = self._simple_tokenize
        elif self.tokenizer == "spacy":
            try:
                import spacy

                nlp = spacy.load("en_core_web_sm")
                self._tokenizer_func = lambda text: [token.text for token in nlp(text)]
            except (ImportError, OSError):
                print("Warning: spacy not available, using simple tokenizer")
                self._tokenizer_func = self._simple_tokenize
        else:
            self._tokenizer_func = self._simple_tokenize

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame, List[Any]]:
        """Chunk text data."""
        self._check_is_fitted()

        # For text processing, keep lists as lists
        if isinstance(X, list):
            list_result = [self._chunk_text(str(x)) for x in X]
            return list_result

        X = self._validate_input(X)

        if isinstance(X, pd.DataFrame):
            df_result = X.copy()
            for col in X.columns:
                df_result[col] = df_result[col].astype(str).apply(self._chunk_text)  # type: ignore
            return df_result
        else:
            if X.ndim == 1:
                array_1d_result: List[List[str]] = [self._chunk_text(str(x)) for x in X]
                return array_1d_result
            else:
                array_2d_result: List[List[List[str]]] = [
                    [self._chunk_text(str(x)) for x in row] for row in X
                ]
                return array_2d_result

    def _simple_tokenize(self, text: str) -> List[str]:
        """Simple tokenization by splitting on whitespace."""
        return text.split()

    def _chunk_text(self, text: str) -> List[str]:
        """Chunk a single text string."""
        if not isinstance(text, str):
            text = str(text)

        if self.strategy == "fixed":
            return self._fixed_chunking(text)
        elif self.strategy == "sentence":
            return self._sentence_chunking(text)
        elif self.strategy == "semantic":
            return self._semantic_chunking(text)
        else:
            raise ValueError(f"Unknown chunking strategy: {self.strategy}")

    def _fixed_chunking(self, text: str) -> List[str]:
        """Fixed-size chunking."""
        if self._tokenizer_func is None:
            raise ValueError("Tokenizer function not initialized")
        tokens = self._tokenizer_func(text)
        chunks = []

        for i in range(0, len(tokens), self.chunk_size - self.chunk_overlap):
            chunk = tokens[i : i + self.chunk_size]
            if chunk:
                chunks.append(" ".join(chunk))

        return chunks

    def _sentence_chunking(self, text: str) -> List[str]:
        """Sentence-based chunking."""
        # Simple sentence splitting
        sentences = re.split(r"[.!?]+", text)
        sentences = [s.strip() for s in sentences if s.strip()]

        chunks = []
        current_chunk: List[str] = []
        current_length = 0

        for sentence in sentences:
            if self._tokenizer_func is None:
                raise ValueError("Tokenizer function not initialized")
            sentence_tokens = self._tokenizer_func(sentence)
            sentence_length = len(sentence_tokens)

            if current_length + sentence_length > self.chunk_size and current_chunk:
                chunks.append(" ".join(current_chunk))
                # Start new chunk with overlap
                overlap_tokens = (
                    current_chunk[-self.chunk_overlap :]
                    if self.chunk_overlap > 0
                    else []
                )
                current_chunk = overlap_tokens + sentence_tokens
                current_length = len(current_chunk)
            else:
                current_chunk.extend(sentence_tokens)
                current_length += sentence_length

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks

    def _semantic_chunking(self, text: str) -> List[str]:
        """Semantic chunking (simplified version)."""
        # For now, use sentence chunking as a proxy for semantic chunking
        # In a full implementation, this would use more sophisticated NLP techniques
        return self._sentence_chunking(text)


class TextEmbedder(FeastTransformer):
    """
    Text embedding transformer using various embedding models.

    Parameters:
        model_name: str, default='sentence-transformers/all-MiniLM-L6-v2'
            Name of the embedding model to use.
        normalize: bool, default=True
            Whether to normalize embeddings.
        batch_size: int, default=32
            Batch size for embedding generation.
        max_length: int, default=512
            Maximum sequence length for the model.
    """

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        normalize: bool = True,
        batch_size: int = 32,
        max_length: int = 512,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.model_name = model_name
        self.normalize = normalize
        self.batch_size = batch_size
        self.max_length = max_length
        self._model: Optional[Any] = None
        self._tokenizer: Optional[Any] = None

    def fit(
        self,
        X: Union[np.ndarray, pd.DataFrame, List[Any]],
        y: Optional[Union[np.ndarray, pd.Series, List[Any]]] = None,
    ) -> "TextEmbedder":
        """Load the embedding model."""
        try:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self.model_name)
        except ImportError:
            raise ImportError(
                "sentence-transformers is required for TextEmbedder. Install with: pip install sentence-transformers"
            )

        self._is_fitted = True
        return self

    def transform(
        self, X: Union[np.ndarray, pd.DataFrame, List[Any]]
    ) -> Union[np.ndarray, pd.DataFrame]:
        """Generate embeddings for text data."""
        self._check_is_fitted()

        X = self._validate_input(X)

        if isinstance(X, pd.DataFrame):
            # Process each column separately
            result = X.copy()
            for col in X.columns:
                texts = X[col].astype(str).tolist()
                embeddings = self._generate_embeddings(texts)
                # Store embeddings as lists in the DataFrame
                result[col] = [emb.tolist() for emb in embeddings]
        else:
            if X.ndim == 1:
                texts = [str(x) for x in X]
                embeddings = self._generate_embeddings(texts)
                result = np.array([emb.tolist() for emb in embeddings])
            else:
                # Flatten 2D array and process
                texts = [str(x) for row in X for x in row]
                embeddings = self._generate_embeddings(texts)
                result = np.array([emb.tolist() for emb in embeddings])

        return result

    def _generate_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """Generate embeddings for a list of texts."""
        if not texts:
            return []

        # Generate embeddings in batches
        embeddings = []
        if self._model is None:
            raise ValueError("Model not initialized")
        for i in range(0, len(texts), self.batch_size):
            batch_texts = texts[i : i + self.batch_size]
            batch_embeddings = self._model.encode(
                batch_texts,
                normalize_embeddings=self.normalize,
                show_progress_bar=False,
            )
            embeddings.extend(batch_embeddings)

        return embeddings

    def get_embedding_dimension(self) -> int:
        """Get the dimension of the embeddings."""
        if not self._is_fitted:
            raise ValueError("TextEmbedder has not been fitted yet.")

        # Get embedding dimension by encoding a dummy text
        if self._model is None:
            raise ValueError("Model not initialized")
        dummy_embedding = self._model.encode(
            ["dummy"], normalize_embeddings=self.normalize
        )
        return dummy_embedding.shape[1]
