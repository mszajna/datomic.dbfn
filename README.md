# Datomic dbfn

Facilities for development of Datomic transaction functions.

Datomic ships with the following transaction functions:

:db/add - idempotently record a datom enforcing schema, uniqueness and referential integrity,
:db/retract - idempotently retract a datom,
:db/retractEntity - :db/retract all attributes of and references to an entity if it exists,
:db/cas - asserts the current value of an entity's attribute of :db.cardinality/one,
          then :db/add or abort transaction.

The set of transaction functions available shapes the way you model your data.


===



## License

This project is distributed under
[The MIT License](https://github.com/mszajna/datomic.dbfn/blob/master/LICENSE).
