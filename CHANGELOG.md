# Changelog

## [1.0.0](https://github.com/pgx-contrib/pgxaip/compare/v0.0.1...v1.0.0) (2026-09-03)


### ⚠ BREAKING CHANGES

* Query.Filter is now *cel.Ast rather than filtering.Filter, and Query.OrderBy / Query.PageToken come from github.com/protoc-contrib/aip-go. Filter expressions are CEL, not AIP-160.
* FilterRewriter, OrderByRewriter, CursorRewriter, ChainRewriter, and the sentinel substitution are removed. Replace usages with pgxaip.Query{...}.Rewrite(). Module now requires github.com/iamralch/aip-go for PageToken.Cursor.

### Features

* take filters as a compiled CEL AST, drop einride ([940a89d](https://github.com/pgx-contrib/pgxaip/commit/940a89d14f29992d4323b549040c80f0a60abd17))


### Bug Fixes

* **deps:** bump pgxcel to published commit and drop local replace ([ff6593c](https://github.com/pgx-contrib/pgxaip/commit/ff6593cb0e5bdff63279e5a40529acccdb81d406))
* **github:** correct action versions in update.yml ([0f517bd](https://github.com/pgx-contrib/pgxaip/commit/0f517bdee0708ea261e13a70f71f55713988c151))


### Code Refactoring

* replace sentinel rewriters with Query.Rewrite ([5e44c18](https://github.com/pgx-contrib/pgxaip/commit/5e44c18dbe57a04b492ff67e85ec7ad2f1b0e055))
