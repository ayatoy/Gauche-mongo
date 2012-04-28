# API

まだ書きかけ。APIも定まってなかったりするんでドラスティックな変更とかもあったりするかもしれません。普通の使用なら`mongo.bson`と`mongo.core`を把握しとけば十分かも。

## [Module] `mongo`

このモジュールは以下のモジュール群を`extend`しているに過ぎません。`(use mongo)`とするだけで低レベルAPIを含め全ての機能を利用することが出来ます。

* mongo.util
* mongo.bson
* mongo.wire
* mongo.node
* mongo.core

## [Module] `mongo.util`

このモジュールは各モジュールを通して一般的に使われる機能や雑多な手続きを定義します。内部的な手続きであったり、実験的な機能はこのモジュールで定義されることが多いです。

### [Condition Type] `<mongo-error>`

全てのエラーコンディションの基底となるコンディション。`<error>`を継承しています。

#### [Field] `reason`

エラーコンディションが返される原因となったコンディションやオブジェクト等に束縛されるフィールド。`mongo-error-reason`によってアクセスすることができます。

### [Function] `mongo-error? obj`

`obj`の型が`<mongo-error>`である場合には`#t`、それ以外であれば`#f`が返されます。

### [Condition Type] `<mongo-parse-error>`

URI等のパースが失敗したときに返されるコンディション。`<mongo-error>`を継承しています。

### [Function] `mongo-parse-error? obj`

`obj`の型が`<mongo-parse-error>`である場合には`#t`、それ以外であれば`#f`が返されます。

### [Function] `alist key value ...`
### [Function] `% key value ...`

連想リストのコンストラクタ。引数の奇数`n`番目をキーとしたときに`n+1`番目が対応した値となります。`%`は`alist`の別名にしか過ぎません。

    (alist "key1" "value1" "key2" "value2")
      => (("key1" . "value1") ("key2" . "value2"))

    (% "key1" (% "$exists" 'false) "key2" (% "$in" #(2 4 6)))
      => (("key1" . (("$exists" . false))) ("key2" . (("$in" . #(2 4 6)))))

### [Function] `alref alst key ...`

連想リスト`alst`から`key`に対応する値を返します。キーが複数渡された場合、ネストされた連想リストを辿ることが出来ます。アクセスの過程でキーが存在しない場合は#fが返されます。キーに対応する値に#fがセットされていた場合と区別がつかないことに注意してください。

    (alref '(("key1" . "value1") ("key2" . "value2")) "key2")
      => "value2"

    (alref '(("key1" . (("$exists" . false))) ("key2" . (("$in" . #(2 4 6)))))
            "key2"
            "$in")
      => #(2 4 6)

    (alref '(("key" . "value")) "foo") => #f
    (alref '(("key" . #f)) "key")      => #f

## [Module] `mongo.bson`

このモジュールは[http://bsonspec.org/](http://bsonspec.org/)で規定される形式をGaucheのオブジェクトとして扱えるようにするものです。いくつかの型はSchemeのネイティブなオブジェクトとして表現され、いくつかは専用のレコード型として定義されます。以下にその対応を示します。

32-bit Integer
:   符号付き32ビットの範囲にあるSchemeの正確な整数

64-bit Integer
:   符号付き64ビットの範囲にあるSchemeの正確な整数

Double
:   Schemeの不正確な実数

UTF-8 string
:   Schemeの完全な文字列

CString
:   バイト値`0`を含まないSchemeの完全な文字列

Null
:   Schemeのシンボル`null`

False
:   Schemeのシンボル`false`

True
:   Schemeのシンボル`true`

Undefined
:   Schemeのシンボル`undefined`

Min key
:   Schemeのシンボル`min`

Max key
:   Schemeのシンボル`max`

Binary
:   レコード型`<bson-binary>`

JavaScrip code
:   レコード型`<bson-code>`

JavaScrip code/scope
:   レコード型`<bson-code/scope>`

ObjectId
:   レコード型`<bson-object-id>`

UTC datetime
:   レコード型`<bson-datetime>`

Regexp
:   レコード型`<bson-regexp>`

DBPointer
:   レコード型`<bson-dbpointer>`

Symbol
:   レコード型`<bson-symbol>`

Timestamp
:   レコード型`<bson-timestamp>`

BSON Document
:   Schemeの連想リスト。キーはCString、値はBSONオブジェクト。

Array
:   要素にBSONオブジェクトを持つSchemeのベクタ

### [Condition type] `<bson-error>`

BSONに関連するエラーコンディションの基底クラスです。`<mongo-error>`を継承しています。

### [Condition type] `<bson-read-error>`

BSONの形式として不正なバイト列を読み込もうとする時に返されるコンディション。`<bson-error>`を継承しています。

### [Condition type] `<bson-write-error>`

BSONオブジェクトとして不正な値を書き込もうとする時に返されるコンディション。`<bson-error>`を継承しています。

### [Condition type] `<bson-construct-error>`

専用のレコード型を持つようなBSONオブジェクトのコンストラクタに不正な値を渡そうとする時に返されるコンディション。`<bson-error>`を継承しています。

### [Record] `<bson-binary>`
#### [Field] `type`
#### [Field] `bytes`
### [Function] `bson-binary? obj`
### [Method] `bson-binary (bytes <u8vector>)`
### [Method] `bson-binary (type <symbol>) (bytes <u8vector>)`

### [Record] `<bson-code>`
#### [Field] `string`
### [Function] `bson-code? obj`
### [Function] `bson-code str`

### [Record] `<bson-code/scope>`
#### [Field] `string`
#### [Field] `scope`
### [Function] `bson-code/scope? obj`
### [Method] `bson-code/scope (str <string>) (scope <list>)`
### [Method] `bson-code (str <string>) (scope <list>)`

### [Record] `<bson-object-id>`
#### [Field] `bytes`
### [Function] `bson-object-id? obj`
### [Function] `bson-object-id-time oid`
### [Function] `bson-object-id-machine oid`
### [Function] `bson-object-id-pid oid`
### [Function] `bson-object-id-inc oid`
### [Function] `bson-object-id time machine pid inc`
### [Method] `bson-object-id`
### [Method] `bson-object-id (str <string>)`
### [Method] `bson-object-id (bytes <u8vector>)`

### [Record] `<bson-datetime>`
#### [Field] `int64`
### [Function] `bson-datetime? obj`
### [Function] `bson-datetime-millisecond dtime`
### [Function] `bson-datetime ms`
### [Method] `bson-datetime`

### [Record] `<bson-regexp>`
#### [Field] `pattern`
#### [Field] `options`
### [Function] `bson-regexp? obj`
### [Method] `bson-regexp (pattern <string>) (options <string>)`

### [Record] `<bson-dbpointer>`
#### [Field] `namespace`
#### [Field] `object-id`
### [Function] `bson-dbpointer? obj`
### [Method] `bson-dbpointer (ns <string>) (oid <bson-object-id)`

### [Record] `<bson-symbol>`
#### [Field] `string`
### [Function] `bson-symbol? obj`
### [Function] `bson-symbol str`

### [Record] `<bson-timestamp>`
#### [Field] `int64`
### [Function] `bson-timestamp? obj`
### [Function] `bson-timestamp-time ts`
### [Function] `bson-timestamp-inc ts`
### [Function] `bson-timestamp time inc`
### [Method] `bson-timestamp time`
### [Method] `bson-timestamp`

## [Module] `mongo.wire`
## [Module] `mongo.node`
## [Module] `mongo.core`
