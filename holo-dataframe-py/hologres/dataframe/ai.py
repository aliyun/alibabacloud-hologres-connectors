"""Lazy wrappers for Hologres built-in AI SQL functions.

Import this module through the package facade::

    import hologres.dataframe as hg

    answer = hg.ai.gen(hg.lit("Summarize: ") + hg.col("body"))

Every helper returns a :class:`~hologres.dataframe.column.Column`; no model is
called until a DataFrame action executes the compiled SQL. Content arguments
follow the DataFrame expression convention: a bare string is a column name,
while a text constant must be wrapped with :func:`hologres.dataframe.lit`.
"""

from __future__ import annotations

from typing import Any

from . import _expr
from .column import Column
from .exceptions import InvalidArgumentError
from .functions import col, lit

__all__ = [
    "chunk",
    "classify",
    "embed",
    "extract",
    "fix_grammar",
    "gen",
    "mask",
    "parse_document",
    "prompt",
    "rank",
    "sentiment",
    "similarity",
    "summarize",
    "to_file",
    "translate",
]


def _content(value: str | Column, name: str) -> _expr.Expression:
    """Turn a content argument into an expression; bare strings name columns."""
    if isinstance(value, Column):
        return value._expr
    if isinstance(value, str) and value:
        return col(value)._expr
    raise InvalidArgumentError(
        f"{name} must be a non-empty column name or Column, got {type(value).__name__}"
    )


def _file_or_bytes(value: str | bytes | Column) -> _expr.Expression:
    """Build an ai_parse_document FILE/BYTEA argument."""
    if isinstance(value, Column):
        return value._expr
    if isinstance(value, str) and value:
        return col(value)._expr
    if isinstance(value, bytes):
        return lit(value)._expr
    raise InvalidArgumentError(
        "file_or_bytes must be a non-empty column name, bytes, or Column"
    )


def _text_literal(value: Any, name: str) -> _expr.Expression:
    """Validate a non-empty configuration string and make it a SQL literal."""
    if not isinstance(value, str) or not value:
        raise InvalidArgumentError(f"{name} must be a non-empty string")
    return lit(value)._expr


def _positive_int(value: Any, name: str) -> _expr.Expression:
    """Validate a positive integer configuration value."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise InvalidArgumentError(f"{name} must be a positive int")
    return lit(value)._expr


def _string_array(value: Any, name: str) -> _expr.Expression:
    """Build a dynamic or literal TEXT[] expression."""
    if isinstance(value, Column):
        return value._expr
    if not isinstance(value, (list, tuple)):
        raise InvalidArgumentError(f"{name} must be a list/tuple of strings or Column")
    if any(not isinstance(item, str) for item in value):
        raise InvalidArgumentError(f"every {name} item must be a string")
    return lit(list(value))._expr


def _literal_arg(value: Any) -> _expr.Expression:
    """Turn a prompt variadic argument into a literal unless already a Column."""
    return value._expr if isinstance(value, Column) else lit(value)._expr


def _call(
    function_name: str,
    arguments: list[_expr.Expression],
    model: str | None,
) -> Column:
    """Build one fixed-name AI function call with an optional model prefix."""
    if model is not None:
        arguments = [_text_literal(model, "model"), *arguments]
    return Column(
        _expr.FunctionCall(
            function_name,
            arguments,
            required_capability=function_name,
        )
    )


def gen(text: str | Column, model: str | None = None) -> Column:
    """Generate text with Hologres ``ai_gen``.

    Args:
        text: Message expression. A string names a column; use ``hg.lit`` for a
            constant prompt. A :func:`prompt` expression is also accepted.
        model: Optional model name. When present it becomes the first SQL
            argument; ``None`` uses Hologres automatic model selection.

    Returns:
        A lazy ``Column`` compiling to ``ai_gen(text)`` or
        ``ai_gen(model, text)``.

    Raises:
        InvalidArgumentError: If ``text`` or ``model`` is invalid.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.ai.gen("question")._to_sql().as_string()
        'ai_gen("question")'
        >>> hg.ai.gen(hg.lit("hello"), model="qwen-plus")._to_sql().as_string()
        "ai_gen('qwen-plus', 'hello')"
    """
    return _call("ai_gen", [_content(text, "text")], model)


def embed(content: str | Column, model: str | None = None) -> Column:
    """Create an embedding with Hologres ``ai_embed``.

    Args:
        content: Text or FILE expression. A string names a column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy ``Column`` containing the ``REAL[]`` embedding expression.

    Raises:
        InvalidArgumentError: If ``content`` or ``model`` is invalid.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.ai.embed("content")._to_sql().as_string()
        'ai_embed("content")'
    """
    return _call("ai_embed", [_content(content, "content")], model)


def rank(src: str | Column, cmp: str | Column, model: str | None = None) -> Column:
    """Score relevance between two texts with Hologres ``ai_rank``.

    Args:
        src: Query/source text expression; a string names a column.
        cmp: Candidate text expression; a string names a column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy floating-point score ``Column``.

    Raises:
        InvalidArgumentError: If an input or ``model`` is invalid.

    Example:
        >>> import hologres.dataframe as hg
        >>> out = hg.ai.rank(hg.lit("query"), "body")._to_sql().as_string()
        >>> out.startswith("ai_rank('query',")
        True
    """
    return _call(
        "ai_rank",
        [_content(src, "src"), _content(cmp, "cmp")],
        model,
    )


def chunk(
    text: str | Column,
    size: int = 300,
    overlap: int = 50,
    separators: list[str] | tuple[str, ...] | Column | None = None,
    model: str | None = None,
) -> Column:
    """Split text with Hologres ``ai_chunk``.

    Args:
        text: Text expression; a string names a column.
        size: Maximum chunk size, as a positive integer.
        overlap: Chunk overlap, as a positive integer.
        separators: Optional literal string sequence or dynamic array ``Column``.
            ``None`` lets Hologres use its default separators.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy ``TEXT[]`` chunk expression.

    Raises:
        InvalidArgumentError: If an argument has an invalid type or value.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.ai.chunk("body", size=100, overlap=10)._to_sql().as_string()
        'ai_chunk("body", 100, 10)'
    """
    args = [
        _content(text, "text"),
        _positive_int(size, "size"),
        _positive_int(overlap, "overlap"),
    ]
    if separators is not None:
        args.append(_string_array(separators, "separators"))
    return _call("ai_chunk", args, model)


def classify(
    content: str | Column,
    labels: list[str] | tuple[str, ...] | Column,
    model: str | None = None,
) -> Column:
    """Classify content into labels with Hologres ``ai_classify``.

    Args:
        content: Text expression; a string names a column.
        labels: Literal label sequence or a dynamic ``TEXT[]`` Column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy text classification ``Column``.

    Raises:
        InvalidArgumentError: If an argument has an invalid type or value.

    Example:
        >>> import hologres.dataframe as hg
        >>> out = hg.ai.classify("description", ["tech", "food"])
        >>> "{tech,food}" in out._to_sql().as_string()
        True
    """
    return _call(
        "ai_classify",
        [_content(content, "content"), _string_array(labels, "labels")],
        model,
    )


def extract(
    content: str | Column,
    labels: list[str] | tuple[str, ...] | Column,
    model: str | None = None,
) -> Column:
    """Extract labeled fields with Hologres ``ai_extract``.

    Args:
        content: Text or FILE expression; a string names a column.
        labels: Literal field-name sequence or a dynamic ``TEXT[]`` Column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy JSON extraction ``Column``.

    Raises:
        InvalidArgumentError: If an argument has an invalid type or value.

    Example:
        >>> import hologres.dataframe as hg
        >>> out = hg.ai.extract("resume", ["name", "email"])
        >>> "{name,email}" in out._to_sql().as_string()
        True
    """
    return _call(
        "ai_extract",
        [_content(content, "content"), _string_array(labels, "labels")],
        model,
    )


def mask(
    content: str | Column,
    labels: list[str] | tuple[str, ...] | Column,
    model: str | None = None,
) -> Column:
    """Mask labeled sensitive data with Hologres ``ai_mask``.

    Args:
        content: Text expression; a string names a column.
        labels: Literal sensitive-field sequence or dynamic ``TEXT[]`` Column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy masked-text ``Column``.

    Raises:
        InvalidArgumentError: If an argument has an invalid type or value.

    Example:
        >>> import hologres.dataframe as hg
        >>> out = hg.ai.mask("body", ["phone"])._to_sql().as_string()
        >>> "{phone}" in out
        True
    """
    return _call(
        "ai_mask",
        [_content(content, "content"), _string_array(labels, "labels")],
        model,
    )


def fix_grammar(content: str | Column, model: str | None = None) -> Column:
    """Correct grammar with Hologres ``ai_fix_grammar``.

    Args:
        content: Text expression; a string names a column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy corrected-text ``Column``.

    Raises:
        InvalidArgumentError: If ``content`` or ``model`` is invalid.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.ai.fix_grammar("body")._to_sql().as_string()
        'ai_fix_grammar("body")'
    """
    return _call("ai_fix_grammar", [_content(content, "content")], model)


def summarize(
    content: str | Column,
    max_words: int = 50,
    model: str | None = None,
) -> Column:
    """Summarize text with Hologres ``ai_summarize``.

    Args:
        content: Text expression; a string names a column.
        max_words: Positive maximum summary length in words.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy summary-text ``Column``.

    Raises:
        InvalidArgumentError: If an argument has an invalid type or value.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.ai.summarize("body", max_words=15)._to_sql().as_string()
        'ai_summarize("body", 15)'
    """
    return _call(
        "ai_summarize",
        [_content(content, "content"), _positive_int(max_words, "max_words")],
        model,
    )


def translate(
    content: str | Column,
    to_lang: str,
    model: str | None = None,
) -> Column:
    """Translate text with Hologres ``ai_translate``.

    Args:
        content: Text expression; a string names a column.
        to_lang: Target language, emitted as a text literal.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy translated-text ``Column``.

    Raises:
        InvalidArgumentError: If an argument has an invalid type or value.

    Example:
        >>> import hologres.dataframe as hg
        >>> out = hg.ai.translate("body", "en")._to_sql().as_string()
        >>> out.endswith(", 'en')")
        True
    """
    return _call(
        "ai_translate",
        [_content(content, "content"), _text_literal(to_lang, "to_lang")],
        model,
    )


def similarity(
    t1: str | Column,
    t2: str | Column,
    model: str | None = None,
) -> Column:
    """Measure text similarity with Hologres ``ai_similarity``.

    Args:
        t1: First text expression; a string names a column.
        t2: Second text expression; a string names a column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy floating-point similarity ``Column``.

    Raises:
        InvalidArgumentError: If an input or ``model`` is invalid.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.ai.similarity("left_text", "right_text")._to_sql().as_string()
        'ai_similarity("left_text", "right_text")'
    """
    return _call(
        "ai_similarity",
        [_content(t1, "t1"), _content(t2, "t2")],
        model,
    )


def sentiment(content: str | Column, model: str | None = None) -> Column:
    """Analyze sentiment with Hologres ``ai_analyze_sentiment``.

    Args:
        content: Text expression; a string names a column.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy sentiment-text ``Column``.

    Raises:
        InvalidArgumentError: If ``content`` or ``model`` is invalid.

    Example:
        >>> import hologres.dataframe as hg
        >>> hg.ai.sentiment("comment")._to_sql().as_string()
        'ai_analyze_sentiment("comment")'
    """
    return _call("ai_analyze_sentiment", [_content(content, "content")], model)


def parse_document(
    file_or_bytes: str | bytes | Column,
    input_format: str = "auto",
    output_format: str = "json",
    model: str | None = None,
) -> Column:
    """Parse a FILE or BYTEA value with Hologres ``ai_parse_document``.

    Args:
        file_or_bytes: FILE ``Column`` (commonly from :func:`to_file`), BYTEA
            bytes, or a string naming a FILE/BYTEA column.
        input_format: Input format literal such as ``"auto"`` or ``"pdf"``.
        output_format: Output format literal such as ``"json"`` or
            ``"markdown"``.
        model: Optional model name, emitted as the first argument.

    Returns:
        A lazy parsed-document text ``Column``.

    Raises:
        InvalidArgumentError: If an argument has an invalid type or value.

    Example:
        >>> import hologres.dataframe as hg
        >>> f = hg.ai.to_file("oss://b/a.pdf", "oss-endpoint", "ram-role")
        >>> out = hg.ai.parse_document(
        ...     f, output_format="markdown"
        ... )._to_sql().as_string()
        >>> out.startswith("ai_parse_document(to_file(")
        True
        >>> out.endswith("'auto', 'markdown')")
        True
    """
    return _call(
        "ai_parse_document",
        [
            _file_or_bytes(file_or_bytes),
            _text_literal(input_format, "input_format"),
            _text_literal(output_format, "output_format"),
        ],
        model,
    )


def to_file(
    oss_url: str,
    endpoint: str,
    role_arn: str,
    etag: str | None = None,
) -> Column:
    """Construct a Hologres FILE value from OSS metadata.

    Args:
        oss_url: Non-empty ``oss://`` object URI.
        endpoint: OSS endpoint literal.
        role_arn: RAM role ARN used by Hologres to obtain OSS credentials.
        etag: Optional object ETag. ``None`` selects the three-argument SQL
            overload; a string selects the four-argument overload.

    Returns:
        A lazy FILE ``Column`` compiling to ``to_file(...)``.

    Raises:
        InvalidArgumentError: If a required value is not a non-empty string, the
            URI is not ``oss://``, or ``etag`` is invalid.

    Example:
        >>> import hologres.dataframe as hg
        >>> file = hg.ai.to_file("oss://b/a.pdf", "oss-endpoint", "ram-role")
        >>> file._to_sql().as_string()
        "to_file('oss://b/a.pdf', 'oss-endpoint', 'ram-role')"
    """
    if (
        not isinstance(oss_url, str)
        or not oss_url.startswith("oss://")
        or oss_url == "oss://"
    ):
        raise InvalidArgumentError("oss_url must be a non-empty oss:// URI")
    arguments = [
        _text_literal(oss_url, "oss_url"),
        _text_literal(endpoint, "endpoint"),
        _text_literal(role_arn, "role_arn"),
    ]
    if etag is not None:
        arguments.insert(1, _text_literal(etag, "etag"))
    return Column(
        _expr.FunctionCall("to_file", arguments, required_capability="to_file")
    )


def prompt(template: str, *args: Any) -> Column:
    """Build a structured multimodal prompt with Hologres ``prompt``.

    Args:
        template: Non-empty format template, emitted as a text literal.
        *args: Values filling the template. ``Column`` values stay expressions;
            all other Python values become SQL literals.

    Returns:
        A lazy JSON prompt ``Column`` suitable for :func:`gen`.

    Raises:
        InvalidArgumentError: If ``template`` is not a non-empty string.

    Example:
        >>> import hologres.dataframe as hg
        >>> out = hg.ai.prompt(
        ...     "Question: {0}", hg.col("question")
        ... )._to_sql().as_string()
        >>> out.startswith("prompt('Question: {0}',")
        True
    """
    expressions = [_text_literal(template, "template")]
    expressions.extend(_literal_arg(arg) for arg in args)
    return Column(
        _expr.FunctionCall("prompt", expressions, required_capability="prompt")
    )
