(ns datomic.dbfn
  (:require [clojure.walk :as w]
            [backtick :as b]
            [datomic.api :as d]))

(def mfunction
  "Memoized d/function, installed on transactor automatically by call.
   Use invalidate-fn-cache to invalidate."
  (memoize d/function))

(def ^:private mf `mfunction)

(defn force-fn
  "Force db-fn created with datomic.api/function to compile immediately."
  [db-fn]
  (force (:fnref db-fn))
  db-fn)

(defn quote-unquotes [form]
  (let [quot (fn [x] `'~x)]
    (w/postwalk
    #(cond
        (b/unquote? %) (quot %)
        (b/unquote-splicing? %) (list* (first %) (map quot (rest %)))
        :else %)
    form)))

(defn ser
  "Serializes the db-fn for use in another txn."
  [db-fn]
  (cond-> (dissoc db-fn :fnref)
    (empty? (:requires db-fn)) (dissoc :requires)
    (empty? (:imports db-fn)) (dissoc :imports)))

(defn quoted-dbfn? [form]
  (and (seq? form)
       (= (first form) 'quote)
       (instance? datomic.function.Function (second form))))

(defn ser-dbfns [form]
  (let [c (gensym "c")
        ser-dbfn-form #(if (quoted-dbfn? %) `(~c (quote ~(ser (second %)))) %)
        updated (w/postwalk ser-dbfn-form form)]
    (if (not= updated form)
     `(let* [~c (resolve '~mf) ~c (if ~c @~c ~`d/function)] ~updated)
      form)))

(defmacro txn
  "Returns a Datomic #db/fn of params that evaluates to body. Note that
   body is not a closure and has to require/import any vars outside
   [clojure.core :refer :all], [datomic.api :as d] and java.lang.* before use
   with the optional requires+imports? map under :requires and :imports.
   Local context can be accessed in compile time using unquote.

   Usage:
   Run #db/fn locally
   => (let [f (txn [db arg] ..)] (f (d/db conn) arg-val))

   Use in transactions once installed
   => (d/transact conn [{:db/ident :my-fn :db/fn (txn [db arg] ..)}])
   => (d/transact conn [[:my-fn arg-val]]) ; db argument is applied implicitly,

   Use in transactions directly with :datomic.dbfn/call (see call in this ns)
   => (d/transact conn [[:datomic.dbfn/call (txn [db arg] ..) arg-val]])

   See also https://docs.datomic.com/on-prem/database-functions.html"
  {:arglists '([requires+imports? [params*] body])}
  [& args]
  (let [[{:keys [requires imports]} params & body]
          (if (map? (first args)) args `(nil ~@args))
        code (if (= 1 (count body)) (first body) `(do ~@body))]
   `(-> {:lang :clojure
         :requires '~requires
         :imports '~imports
         :params '~params
         :code (pr-str (ser-dbfns (b/template ~(quote-unquotes code))))}
        d/function
        force-fn)))

(defmacro deftxn
  "Same as (def name doc-string? (txn [params*] body))."
  {:arglists '([name doc-string? requires+imports? [params*] body])}
  [name & args]
  (let [[doc & args] (if (string? (first args)) args `(nil ~@args))]
   `(def ~name ~@(when doc [doc]) (txn ~@args))))

(defn- ns-sym [s] (symbol (namespace s)))
(defn- name-sym [s] (symbol (name s)))

(deftxn call
  "Compiles the db-fn using datomic.api/function and applies it db and args.
  Caches compilation on the transactor indefinitely, unless invalidated
  with invalidate-fn-cache.

  Usage:
  => (require '[datomic.dbfn :as t])
  => (d/transact conn [t/call-install-tx]) ; run once
  => (t/deftxn my-tx [db arg] ..)
  => (d/transact conn [[::t/call my-tx arg-val]])"
  [db dbfn & args]
  (let [compile-dbfn
        @(or (resolve ~mf)
              (intern (create-ns ~(ns-sym mf)) ~(ns-sym mf)
                      (memoize d/function)))]
    (apply (compile-dbfn dbfn) db args)))

(deftxn invalidate-fn-cache
  "Invalidate the compilation cache of call. Datomic serializes #db/fn to a map
  which is then used as the cache key guaranteeing that the cache never goes
  stale. #db/fns don't close over the lexical scope and so tend not to occupy
  much memory either. You should rarely need this function.

  Usage:
  => (d/transact conn [[:datomic.dbfn/call invalidate-dbfn-cache]])"
  [_]
  (ns-unmap (create-ns ~(ns-sym mf)) ~(name-sym mf)))

(def call-install-tx
  "Transaction statement to install :datomic.dbfn/call, see call in this namespace."
  {:db/ident ::call :db/fn call})
