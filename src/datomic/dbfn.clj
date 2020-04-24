(ns datomic.dbfn
  (:refer-clojure :exclude [fn defn])
  (:require [datomic.api :as d]
            [datomic.function :refer [compile-clojure]]))

(clojure.core/defn fn-spec
  "Given quoted: map m of requires and imports, params and Clojure code,
   attempts to compile the code to catch any problems and returns a map
   suitable for datomic.api/function."
  [m params code]
  (let [{:keys [imports requires]} m]
    (when (empty? params)
      (throw (new IllegalArgumentException
                  "#db/fn needs at least one argument for db value.")))
    (compile-clojure imports requires params (str code))
    (assoc m :lang "clojure" :params params :code code)))

(defmacro fn
  "Returns a Datomic #db/fn of params that evaluates to body. Note that
   body is not a closure and has to require/import any vars outside
   [clojure.core :refer :all], [datomic.api :as d] and java.lang.* before use
   with the optional requires+imports? map under :requires and :imports.

   Usage:
   Run #db/fn locally
   => (let [f (dbfn/fn [db arg] ...)] (f (d/db conn) arg-val))

   Use in transactions once installed
   => (d/transact conn [{:db/ident :my-fn :db/fn (dbfn/fn [db arg] ...)}])
   => (d/transact conn [[:myfn arg-val]]) ; db argument is applied implicitly,

   Use in transactions directly with :datomic.dbfn/call (see call in this ns)
   => (dbfn/def myfn [db arg] ...)
   => (d/transact conn [[::dbfn/call myfn arg-val]])

   See also https://docs.datomic.com/on-prem/database-functions.html"
  {:arglists '([requires+imports? [params*] body])}
  [& args]
  (let [[m params & body] (if (map? (first args)) args `(nil ~@args))
        code (if (= 1 (count body)) (first body) `(do ~@body))]
   `(d/function (fn-spec '~m '~params '~code))))

(defmacro defn
  "Same as (def name doc-string? (datomic.dbfn/fn [params*] body))."
  {:arglists '([name doc-string? requires+imports? [params*] body])}
  [name & args]
  (let [[doc & args] (if (string? (first args)) args `(nil ~@args))]
   `(def ~name ~@(when doc [doc]) (datomic.dbfn/fn ~@args))))

(datomic.dbfn/defn call
  "Compiles the db-fn using datomic.api/function and applies it db and args.
   Caches compilation on the transactor indefinitely, unless invalidated
   with invalidate-dbfn-cache.

   Usage:
   => (d/transact conn [dbfn/call-install-tx]) ; run once
   => (d/transact conn [[::dbfn/call (dbfn [db arg] ...) arg-val]])"
  [db db-fn & args]
  (let [compile
        @(or (resolve 'datomic.dbfn/dbfn-cache)
             (intern (create-ns 'datomic.dbfn)
                     'dbfn-cache
                     (memoize d/function)))]
    (apply (compile db-fn) db args)))

(datomic.dbfn/defn invalidate-dbfn-cache
  "Invalidate the compilation cache of :datomic.dbfn/call. Datomic serializes
   #db/fn to a map which is then used as the cache key guaranteeing that the
   cache never goes stale. #db/fns don't close over the lexical scope and so
   tend not to occupy much memory either. You should rarely need this function.
   
   Usage:
   => (d/transact conn [[::dbfn/call dbfn/invalidate-dbfn-cache]])"
  [_]
  (ns-unmap 'datomic.dbfn 'dbfn-cache))

(def call-install-tx
  "Transaction statement to install :datomic.dbfn/call, see call in this namespace."
  {:db/ident :datomic.dbfn/call :db/fn call})
