# SQL Query Engine — Paper Source

LaTeX source for the technical report:

**SQL Query Engine: A Self-Healing LLM Pipeline for Natural Language to PostgreSQL Translation**
Muhammad Adeel Ijaz (2026)

- **Paper**: [arXiv:2604.16511](https://arxiv.org/abs/2604.16511)
- **Source Code**: [main branch](https://github.com/codeadeel/sqlqueryengine/tree/main)
- **Dataset**: [Hugging Face](https://huggingface.co/datasets/codeadeel/sql-query-engine-synthetic)

## Files

| File | Description |
|---|---|
| `main.tex` | Paper source |
| `references.bib` | Bibliography |
| `main.bbl` | Compiled bibliography |
| `arxiv.sty` | arXiv style template |
| `orcid.pdf` | ORCID icon (used by arxiv.sty) |

## Building

```bash
pdflatex main.tex
bibtex main
pdflatex main.tex
pdflatex main.tex
```

## Citation

```bibtex
@article{ijaz2026sqlqueryengine,
  title={SQL Query Engine: A Self-Healing LLM Pipeline for Natural Language to PostgreSQL Translation},
  author={Ijaz, Muhammad Adeel},
  journal={arXiv preprint arXiv:2604.16511},
  year={2026}
}
```
