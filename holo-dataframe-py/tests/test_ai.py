"""Offline tests for the ``hg.ai`` Hologres AI function wrappers."""

from __future__ import annotations

import pytest
from hologres.table_name import TableName

import hologres.dataframe as hg
from hologres.dataframe import _plan
from hologres.dataframe.column import Column
from hologres.dataframe.dataframe import DataFrame


def _sql(column: Column) -> str:
    return column._to_sql().as_string()


class TestAiNamespace:
    def test_namespace_is_reachable_from_facade(self):
        assert hg.ai.__name__ == "hologres.dataframe.ai"

    def test_every_export_resolves(self):
        assert [name for name in hg.ai.__all__ if not hasattr(hg.ai, name)] == []

    def test_all_has_no_duplicates(self):
        assert len(hg.ai.__all__) == len(set(hg.ai.__all__))


class TestCoreFunctions:
    def test_gen_without_and_with_model(self):
        assert _sql(hg.ai.gen("question")) == 'ai_gen("question")'
        assert _sql(hg.ai.gen(hg.lit("hello"), model="qwen-plus")) == (
            "ai_gen('qwen-plus', 'hello')"
        )

    def test_embed(self):
        assert _sql(hg.ai.embed("content")) == 'ai_embed("content")'
        assert _sql(hg.ai.embed("content", "embed-model")) == (
            "ai_embed('embed-model', \"content\")"
        )

    def test_rank(self):
        assert _sql(hg.ai.rank(hg.lit("query"), "body")) == (
            "ai_rank('query', \"body\")"
        )

    def test_chunk_defaults_and_separators(self):
        assert _sql(hg.ai.chunk("body")) == 'ai_chunk("body", 300, 50)'
        assert (
            _sql(
                hg.ai.chunk(
                    "body",
                    size=100,
                    overlap=10,
                    separators=("|", " "),
                    model="chunk-model",
                )
            )
            == "ai_chunk('chunk-model', \"body\", 100, 10, '{|,\" \"}')"
        )

    def test_chunk_dynamic_separators(self):
        assert _sql(hg.ai.chunk("body", separators=hg.col("seps"))) == (
            'ai_chunk("body", 300, 50, "seps")'
        )

    @pytest.mark.parametrize(
        ("builder", "expected"),
        [
            (
                lambda: hg.ai.classify("description", ["tech", "food"]),
                "ai_classify(\"description\", '{tech,food}')",
            ),
            (
                lambda: hg.ai.extract("resume", ("name", "email")),
                "ai_extract(\"resume\", '{name,email}')",
            ),
            (
                lambda: hg.ai.mask("body", ["phone"]),
                "ai_mask(\"body\", '{phone}')",
            ),
            (
                lambda: hg.ai.fix_grammar("body"),
                'ai_fix_grammar("body")',
            ),
            (
                lambda: hg.ai.summarize("body", max_words=15),
                'ai_summarize("body", 15)',
            ),
            (
                lambda: hg.ai.translate("body", "en"),
                "ai_translate(\"body\", 'en')",
            ),
            (
                lambda: hg.ai.similarity("left_text", "right_text"),
                'ai_similarity("left_text", "right_text")',
            ),
            (
                lambda: hg.ai.sentiment("comment"),
                'ai_analyze_sentiment("comment")',
            ),
        ],
    )
    def test_named_wrappers(self, builder, expected):
        assert _sql(builder()) == expected

    def test_dynamic_labels_and_model_prefix(self):
        assert (
            _sql(hg.ai.classify("body", hg.col("labels"), model="classifier"))
            == 'ai_classify(\'classifier\', "body", "labels")'
        )

    def test_model_prefix_on_multi_argument_functions(self):
        assert _sql(hg.ai.similarity("a", "b", model="sim")) == (
            'ai_similarity(\'sim\', "a", "b")'
        )
        assert _sql(hg.ai.summarize("body", model="sum")) == (
            "ai_summarize('sum', \"body\", 50)"
        )


class TestFileAndPromptFunctions:
    def test_to_file_three_and_four_argument_overloads(self):
        assert (
            _sql(hg.ai.to_file("oss://bucket/a.pdf", "oss-endpoint", "ram-role"))
            == "to_file('oss://bucket/a.pdf', 'oss-endpoint', 'ram-role')"
        )
        assert (
            _sql(
                hg.ai.to_file(
                    "oss://bucket/a.pdf",
                    "oss-endpoint",
                    "ram-role",
                    etag="abc",
                )
            )
            == "to_file('oss://bucket/a.pdf', 'abc', 'oss-endpoint', 'ram-role')"
        )

    def test_to_file_escapes_values(self):
        out = _sql(hg.ai.to_file("oss://b/a'b", "end'point", "role'arn"))
        assert out == "to_file('oss://b/a''b', 'end''point', 'role''arn')"

    def test_parse_document_file_column(self):
        file = hg.ai.to_file("oss://bucket/a.pdf", "endpoint", "role")
        assert _sql(
            hg.ai.parse_document(file, output_format="markdown", model="parser")
        ) == (
            "ai_parse_document('parser', to_file('oss://bucket/a.pdf', "
            "'endpoint', 'role'), 'auto', 'markdown')"
        )

    def test_parse_document_named_column_and_bytes(self):
        assert _sql(hg.ai.parse_document("file_col")) == (
            "ai_parse_document(\"file_col\", 'auto', 'json')"
        )
        out = _sql(hg.ai.parse_document(b"pdf"))
        assert out.startswith("ai_parse_document(")
        assert "::bytea" in out
        assert out.endswith(", 'auto', 'json')")

    def test_prompt_keeps_columns_and_literalizes_scalars(self):
        assert (
            _sql(hg.ai.prompt("Question: {0}, n={1}", hg.col("question"), 2))
            == "prompt('Question: {0}, n={1}', \"question\", 2)"
        )
        assert _sql(hg.ai.prompt("Value: {0}", "literal")) == (
            "prompt('Value: {0}', 'literal')"
        )

    def test_prompt_composes_with_gen(self):
        file = hg.ai.to_file("oss://bucket/a.jpg", "endpoint", "role")
        out = _sql(hg.ai.gen(hg.ai.prompt("Describe {0}", file), model="vision"))
        assert out == (
            "ai_gen('vision', prompt('Describe {0}', "
            "to_file('oss://bucket/a.jpg', 'endpoint', 'role')))"
        )


class TestComposition:
    def test_ai_columns_compose_in_dataframe_plan(self):
        df = DataFrame(_plan.TableScan(TableName.valueOf("public.docs")))
        out = (
            df.select(
                "id",
                hg.ai.embed("body").alias("vec"),
                hg.ai.sentiment("body").alias("sentiment"),
            )
            ._to_sql()
            .as_string()
        )
        assert out == (
            'SELECT "id", ai_embed("body") AS "vec", '
            'ai_analyze_sentiment("body") AS "sentiment" FROM "public"."docs"'
        )

    def test_ai_score_can_filter_and_sort(self):
        df = DataFrame(_plan.TableScan(TableName.valueOf("public.docs")))
        score = hg.ai.rank(hg.lit("query"), "body")
        out = df.filter(score > 0.5).order_by(score.desc())._to_sql().as_string()
        assert "WHERE (ai_rank('query', \"body\") > 0.5)" in out
        assert "ORDER BY ai_rank('query', \"body\") DESC" in out


class TestValidation:
    @pytest.mark.parametrize("value", [None, "", 1])
    def test_content_must_be_column_or_non_empty_name(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="text must"):
            hg.ai.gen(value)

    @pytest.mark.parametrize("model", ["", 1, False])
    def test_model_must_be_non_empty_string(self, model):
        with pytest.raises(hg.InvalidArgumentError, match="model"):
            hg.ai.embed("body", model=model)

    @pytest.mark.parametrize("value", [0, -1, 1.5, True])
    def test_chunk_size_must_be_positive_int(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="size"):
            hg.ai.chunk("body", size=value)

    @pytest.mark.parametrize("value", [0, -1, "1", False])
    def test_chunk_overlap_must_be_positive_int(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="overlap"):
            hg.ai.chunk("body", overlap=value)

    @pytest.mark.parametrize("value", [None, "labels", 1])
    def test_labels_must_be_sequence_or_column(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="labels"):
            hg.ai.classify("body", value)

    def test_label_items_must_be_strings(self):
        with pytest.raises(hg.InvalidArgumentError, match="every labels"):
            hg.ai.extract("body", ["name", 1])

    def test_separator_items_must_be_strings(self):
        with pytest.raises(hg.InvalidArgumentError, match="every separators"):
            hg.ai.chunk("body", separators=[" ", 1])

    @pytest.mark.parametrize("value", [0, -1, 1.5, True])
    def test_max_words_must_be_positive_int(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="max_words"):
            hg.ai.summarize("body", max_words=value)

    @pytest.mark.parametrize("value", ["", None, 1])
    def test_translate_language_must_be_non_empty_string(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="to_lang"):
            hg.ai.translate("body", value)

    @pytest.mark.parametrize("value", [None, "", 1, bytearray(b"x")])
    def test_parse_document_input_must_be_file_or_bytes(self, value):
        with pytest.raises(hg.InvalidArgumentError, match="file_or_bytes"):
            hg.ai.parse_document(value)

    @pytest.mark.parametrize("name", ["input_format", "output_format"])
    def test_parse_document_formats_must_be_non_empty_strings(self, name):
        kwargs = {name: ""}
        with pytest.raises(hg.InvalidArgumentError, match=name):
            hg.ai.parse_document("file_col", **kwargs)

    @pytest.mark.parametrize("oss_url", ["", "oss://", "/tmp/a.pdf", None])
    def test_to_file_requires_oss_uri(self, oss_url):
        with pytest.raises(hg.InvalidArgumentError, match="oss_url"):
            hg.ai.to_file(oss_url, "endpoint", "role")

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"endpoint": ""}, "endpoint"),
            ({"role_arn": None}, "role_arn"),
            ({"etag": 1}, "etag"),
        ],
    )
    def test_to_file_validates_metadata(self, kwargs, message):
        values = {"endpoint": "endpoint", "role_arn": "role", **kwargs}
        with pytest.raises(hg.InvalidArgumentError, match=message):
            hg.ai.to_file("oss://bucket/a", **values)

    @pytest.mark.parametrize("template", ["", None, 1])
    def test_prompt_requires_template(self, template):
        with pytest.raises(hg.InvalidArgumentError, match="template"):
            hg.ai.prompt(template)
