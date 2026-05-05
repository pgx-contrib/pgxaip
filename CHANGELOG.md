# Changelog

## [1.0.0](https://github.com/pgx-contrib/pgxaip/compare/v0.0.1...v1.0.0) (2026-05-05)


### ⚠ BREAKING CHANGES

* FilterRewriter, OrderByRewriter, CursorRewriter, ChainRewriter, and the sentinel substitution are removed. Replace usages with pgxaip.Query{...}.Rewrite(). Module now requires github.com/iamralch/aip-go for PageToken.Cursor.

### Bug Fixes

* **deps:** bump pgxcel to published commit and drop local replace ([ff6593c](https://github.com/pgx-contrib/pgxaip/commit/ff6593cb0e5bdff63279e5a40529acccdb81d406))
* **github:** correct action versions in update.yml ([0f517bd](https://github.com/pgx-contrib/pgxaip/commit/0f517bdee0708ea261e13a70f71f55713988c151))


### Code Refactoring

* replace sentinel rewriters with Query.Rewrite ([5e44c18](https://github.com/pgx-contrib/pgxaip/commit/5e44c18dbe57a04b492ff67e85ec7ad2f1b0e055))
