# Branching Strategy

## Purpose

This document describes the branching strategy for the Databricks eCommerce Lakehouse project.

The goal is to support safe development, review, and deployment of notebook, SQL, and documentation changes.

---

## Branches

### main

The `main` branch contains stable project code and documentation.

Only reviewed and tested changes should be merged into `main`.

### Feature Branches

Feature branches are used for new development.

Example branch names:

```text
feature/phase2-inventory-pipeline
feature/phase3-mlflow-model-serving
feature/phase4-genai-rag
feature/phase5-governance
```

---

## Recommended Workflow

1. Create a feature branch from `main`.
2. Make notebook, SQL, or documentation changes.
3. Commit changes with clear commit messages.
4. Open a pull request into `main`.
5. Review the changes.
6. Confirm CI checks pass.
7. Merge into `main`.

---

## Pull Request Expectations

A pull request should include:

- Summary of changes
- Reason for the change
- Screenshots if UI evidence is required
- Notes about Databricks jobs or monitoring updates
- Confirmation that sensitive tokens or credentials were not committed

---

## Example Commit Messages

```text
Add governance SQL scripts
Add service principal creation script
Add lakehouse monitoring documentation
Update README with Phase 5 governance
```

---

## Protection Recommendation

For a production repository, the `main` branch should require:

- Pull request review
- Passing CI checks
- No direct commits to `main`
- No committed secrets
