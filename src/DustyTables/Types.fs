namespace DustyTables

open System
open System.Data

[<RequireQualifiedAccess>]
type SqlValue =
    | TinyInt of uint8
    | Smallint of int16
    | Int of int
    | Bigint of int64
    | String of string
    | DateTime of DateTime
    | DateTimeTZ of DateTime
    | DateTimeOffset of DateTimeOffset
    | Bit of bool
    | Bool of bool
    | Float of double
    | Decimal of decimal
    | Binary of byte[]
    | UniqueIdentifier of Guid
    | UniqueIdentifierArray of Guid []
    | Table of string * DataTable
    | StringArray of string array
    | IntArray of int array
    | Null
