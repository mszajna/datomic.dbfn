# Datomic dbfn

Facilities for development of Datomic transaction functions.

Datomic ships with the following primitives:

:db/add - idempotently record a datom enforcing schema, uniqueness and referential integrity,
:db/retract - idempotently retract a datom,

and transaction functions:
:db/retractEntity - :db/retract all attributes of and references to an entity if it exists,
:db/cas - asserts the current value of an entity's attribute of :db.cardinality/one,
          then :db/add or abort transaction.

The set of transaction functions available shapes the way you model your data.

===

## Motivation for custom txn macro

It is frustrating how each transaction function definition is an island, with its own
objects, abstractions and dependencies. You can obviously still call previously installed
fns generating adequate tx statement.

If you wanted to reuse db/fn code using d/function you'd build quoted snippets
of code and embed them using syntax unquote. You could also unquote function calls
generating snippets, which is exactly what macros do in regular code.

Resolving macros on the client is viable and could allow some code reuse but is
quite limiting. Most third-party macros resolve to some function call you can't
expect to be available on the transactor. There is pure macros like -> but they're
first party anyway (available on transactor too).

## License

This project is distributed under
[The MIT License](https://github.com/mszajna/datomic.dbfn/blob/master/LICENSE).
