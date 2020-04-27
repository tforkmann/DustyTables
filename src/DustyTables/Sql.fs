namespace DustyTables

open System
open System.Threading.Tasks
open System.Data
open Microsoft.Data.SqlClient

type SqlRow = list<string * SqlValue>

type SqlTable = list<SqlRow>

[<RequireQualifiedAccess>]
module Sql =
    open FSharp.Control.Tasks.V2.ContextInsensitive

    type SqlProps = private {
        ConnectionString : string
        SqlQuery : string option
        Parameters : SqlRow
        IsFunction : bool
        Timeout: int option
        NeedPrepare : bool
    }

    let private defaultProps() = {
        ConnectionString = "";
        SqlQuery = None
        Parameters = [];
        IsFunction = false
        NeedPrepare = false
        Timeout = None
    }

    let connect constr  = { defaultProps() with ConnectionString = constr }

    let query (sqlQuery: string) props = { props with SqlQuery = Some sqlQuery }
    let queryStatements (sqlQuery: string list) props = { props with SqlQuery = Some (String.concat "\n" sqlQuery) }
    let storedProcedure (sqlQuery: string) props = { props with SqlQuery = Some sqlQuery; IsFunction = true }
    let prepare  props = { props with NeedPrepare = true}
    let parameters ls props = { props with Parameters = ls }
    let timeout n props = { props with Timeout = Some n }

    let toBool = function
        | SqlValue.Bool x -> x
        | value -> failwithf "Could not convert %A into a boolean value" value

    let toTinyint = function
        | SqlValue.TinyInt x -> x
        | value -> failwithf "Could not convert %A into a tinyint" value

    let toSmallint = function
        | SqlValue.Smallint x -> x
        | value -> failwithf "Could not convert %A into a short (int16)" value

    let toInt = function
        | SqlValue.Int x -> x
        | value -> failwithf "Could not convert %A into an integer" value

    let toBigint = function
        | SqlValue.Bigint x -> x
        | value -> failwithf "Could not convert %A into long (int64)" value

    let toString = function
        | SqlValue.String x -> x
        | value -> failwithf "Could not convert %A into a string" value

    let toDateTime = function
        | SqlValue.DateTime x -> x
        | value -> failwithf "Could not convert %A into a DateTime" value

    let toDateTimeOffset = function
        | SqlValue.DateTimeOffset x -> x
        | value -> failwithf "Could not convert %A into a DateTimeOffset" value

    let toFloat = function
        | SqlValue.Float x -> x
        | value -> failwithf "Could not convert %A into a floating number" value

    let toBinary = function
        | SqlValue.Binary bytes -> bytes
        | value -> failwithf "Could not convert %A into binary (i.e. byte[]) value" value

    let toDecimal = function
        | SqlValue.Decimal value -> value
        | value -> failwithf "Could not convert %A into decimal value" value

    let toUniqueIdentifier = function
        | SqlValue.UniqueIdentifier guid -> guid
        | value ->  failwithf "Could not convert %A into Guid value" value

    let readValue value =
        let valueType = value.GetType()
        if isNull value
        then SqlValue.Null
        elif valueType = typeof<uint8>
        then SqlValue.TinyInt (unbox<uint8> value)
        elif valueType = typeof<int16>
        then SqlValue.Smallint (unbox<int16> value)
        elif valueType = typeof<int32>
        then SqlValue.Int (unbox<int32> value)
        elif valueType = typeof<int64>
        then SqlValue.Bigint (unbox<int64> value)
        elif valueType = typeof<bool>
        then SqlValue.Bool (unbox<bool> value)
        elif valueType = typeof<float>
        then SqlValue.Float (unbox<float> value)
        elif valueType = typeof<decimal>
        then SqlValue.Decimal (unbox<decimal> value)
        elif valueType = typeof<DateTime>
        then SqlValue.DateTime (unbox<DateTime> value)
        elif valueType = typeof<DateTimeOffset>
        then SqlValue.DateTimeOffset (unbox<DateTimeOffset> value)
        elif valueType = typeof<byte[]>
        then SqlValue.Binary (unbox<byte[]> value)
        elif valueType = typeof<string>
        then SqlValue.String (unbox<string> value)
        elif valueType = typeof<Guid>
        then SqlValue.UniqueIdentifier (unbox<Guid> value)
        else failwithf "Could not convert value of type '%s' to SqlValue" (valueType.FullName)

    let readTinyInt name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.TinyInt value) -> Some value
            | _ -> None

    let readSmallInt name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Smallint value) -> Some value
            | _ -> None

    let readInt name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Int value) -> Some value
            | _ -> None

    let readBigInt name (row: SqlRow)  =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Bigint value) -> Some value
            | _ -> None

    let readString name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.String value) -> Some value
            | _ -> None

    let readDateTime name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.DateTime value) -> Some value
            | _ -> None

    let readBool name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Bool value) -> Some value
            | _ -> None

    let readDecimal name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Decimal value) -> Some value
            | _ -> None

    let readFloat name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.Float value) -> Some value
            | _ -> None

    let readDateTimeOffset name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.DateTimeOffset value) -> Some value
            | _ -> None

    let readUniqueIdentifier name (row: SqlRow) =
        row
        |> List.tryFind (fun (colName, value) -> colName = name)
        |> Option.map snd
        |> function
            | Some (SqlValue.UniqueIdentifier value) -> Some value
            | _ -> None

    let readRow (reader : SqlDataReader) : SqlRow =

        let readFieldSync fieldIndex =

            let fieldName = reader.GetName(fieldIndex)
            if reader.IsDBNull(fieldIndex)
            then fieldName, SqlValue.Null
            else fieldName, readValue (reader.GetFieldValue(fieldIndex))

        [0 .. reader.FieldCount - 1]
        |> List.map readFieldSync

    let readRowTask (reader: SqlDataReader) =
        let readValueTask fieldIndex =
          task {
              let fieldName = reader.GetName fieldIndex
              let! isNull = reader.IsDBNullAsync fieldIndex
              if isNull then
                return fieldName, SqlValue.Null
              else
                let! value = reader.GetFieldValueAsync fieldIndex
                return fieldName, readValue value
          }

        [0 .. reader.FieldCount - 1]
        |> List.map readValueTask
        |> Task.WhenAll

    let readRowAsync (reader: SqlDataReader) =
        readRowTask reader
        |> Async.AwaitTask

    let readTable (reader: SqlDataReader) : SqlTable =
        [ while reader.Read() do yield readRow reader ]

    let readTableTask (reader: SqlDataReader) =
        let rec readRows rows = task {
            let! canRead = reader.ReadAsync()
            if canRead then
              let! row = readRowTask reader
              return! readRows (List.ofArray row :: rows)
            else
              return rows
        }
        readRows []

    let readTableAsync (reader: SqlDataReader) =
        readTableTask reader
        |> Async.AwaitTask

    let populateRow (cmd: SqlCommand) (row: SqlRow) =
        for param in row do
            let paramValue : obj =
                match snd param with
                | SqlValue.String text -> upcast text
                | SqlValue.TinyInt x -> upcast x
                | SqlValue.Smallint x -> upcast x
                | SqlValue.Int i -> upcast i
                | SqlValue.Bigint x -> upcast x
                | SqlValue.DateTime date -> upcast date
                | SqlValue.Float n -> upcast n
                | SqlValue.Bool b -> upcast b
                | SqlValue.Decimal x -> upcast x
                | SqlValue.Null -> upcast DBNull.Value
                | SqlValue.Binary bytes -> upcast bytes
                | SqlValue.UniqueIdentifier guid -> upcast guid
                | SqlValue.DateTimeOffset x -> upcast x
                | SqlValue.Table (_, x) -> upcast x

            // prepend param name with @ if it doesn't already
            let paramName =
                if (fst param).StartsWith("@")
                then fst param
                else sprintf "@%s" (fst param)

            match snd param with
            | SqlValue.Table (typeName, _) ->
                let tableParam = cmd.Parameters.AddWithValue(paramName, paramValue)
                // TypeName must be set to the custom SQL tvp type
                tableParam.TypeName <- typeName
                // SqlDbType must be set to Structured for a table-valued parameter
                tableParam.SqlDbType <- SqlDbType.Structured
            | _ ->
                cmd.Parameters.AddWithValue(paramName, paramValue) |> ignore

    let private populateCmd (cmd: SqlCommand) (props: SqlProps) =
        if props.IsFunction then cmd.CommandType <- CommandType.StoredProcedure

        match props.Timeout with
        | Some timeout -> cmd.CommandTimeout <- timeout
        | None -> ()

        populateRow cmd props.Parameters

    let executeReader (read: SqlDataReader -> Option<'t>) (props: SqlProps) : 't list =
        if Option.isNone props.SqlQuery then failwith "No query provided to execute"
        use connection = new SqlConnection(props.ConnectionString)
        connection.Open()
        use command = new SqlCommand(Option.get props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        use reader = command.ExecuteReader()
        let rows = ResizeArray<'t>()
        while reader.Read() do
            reader
            |> read
            |> Option.iter rows.Add
        List.ofSeq rows

    let executeTransaction queries (props: SqlProps)  =
        if List.isEmpty queries
        then [ ]
        else
        use connection = new SqlConnection(props.ConnectionString)
        connection.Open()
        use transaction = connection.BeginTransaction()
        let affectedRowsByQuery = ResizeArray<int>()
        for (query, parameterSets) in queries do
            if List.isEmpty parameterSets
            then
                use command = new SqlCommand(query, connection, transaction)
                let affectedRows = command.ExecuteNonQuery()
                affectedRowsByQuery.Add affectedRows
            else
              for parameterSet in parameterSets do
                  use command = new SqlCommand(query, connection, transaction)
                  populateRow command parameterSet
                  let affectedRows = command.ExecuteNonQuery()
                  affectedRowsByQuery.Add affectedRows
        transaction.Commit()
        List.ofSeq affectedRowsByQuery

    let executeTransactionAsync queries (props: SqlProps)  =
        async {
            if List.isEmpty queries
            then return [ ]
            else
            use connection = new SqlConnection(props.ConnectionString)
            do! Async.AwaitTask (connection.OpenAsync())
            use transaction = connection.BeginTransaction()
            let affectedRowsByQuery = ResizeArray<int>()
            for (query, parameterSets) in queries do
                if List.isEmpty parameterSets
                then
                    use command = new SqlCommand(query, connection, transaction)
                    let! affectedRows = Async.AwaitTask(command.ExecuteNonQueryAsync())
                    affectedRowsByQuery.Add affectedRows
                else
                  for parameterSet in parameterSets do
                      use command = new SqlCommand(query, connection, transaction)
                      populateRow command parameterSet
                      let! affectedRows = Async.AwaitTask(command.ExecuteNonQueryAsync())
                      affectedRowsByQuery.Add affectedRows
            transaction.Commit()
            return List.ofSeq affectedRowsByQuery
        }


    let executeTransactionSafe queries (props: SqlProps) =
        try
            if List.isEmpty queries
            then Ok [ ]
            else
            use connection = new SqlConnection(props.ConnectionString)
            connection.Open()
            use transaction = connection.BeginTransaction()
            let affectedRowsByQuery = ResizeArray<int>()
            for (query, parameterSets) in queries do
                if List.isEmpty parameterSets
                then
                    use command = new SqlCommand(query, connection, transaction)
                    let affectedRows = command.ExecuteNonQuery()
                    affectedRowsByQuery.Add affectedRows
                else
                    for parameterSet in parameterSets do
                        use command = new SqlCommand(query, connection, transaction)
                        populateRow command parameterSet
                        let affectedRows = command.ExecuteNonQuery()
                        affectedRowsByQuery.Add affectedRows
            transaction.Commit()
            Ok (List.ofSeq affectedRowsByQuery)
        with
        | ex -> Error ex

    let executeTransactionTask queries (props: SqlProps)  =
        task {
            if List.isEmpty queries
            then return [ ]
            else
            use connection = new SqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use transaction = connection.BeginTransaction()
            let affectedRowsByQuery = ResizeArray<int>()
            for (query, parameterSets) in queries do
                if List.isEmpty parameterSets
                then
                    use command = new SqlCommand(query, connection, transaction)
                    let! affectedRows = command.ExecuteNonQueryAsync()
                    affectedRowsByQuery.Add affectedRows
                else
                  for parameterSet in parameterSets do
                      use command = new SqlCommand(query, connection, transaction)
                      populateRow command parameterSet
                      let! affectedRows = command.ExecuteNonQueryAsync()
                      affectedRowsByQuery.Add affectedRows
            transaction.Commit()
            return List.ofSeq affectedRowsByQuery
        }

    let executeTransactionSafeTask queries (props: SqlProps)  =
        task {
            try
                if List.isEmpty queries
                then return Ok [ ]
                else
                use connection = new SqlConnection(props.ConnectionString)
                do! connection.OpenAsync()
                use transaction = connection.BeginTransaction()
                let affectedRowsByQuery = ResizeArray<int>()
                for (query, parameterSets) in queries do
                    if List.isEmpty parameterSets
                    then
                        use command = new SqlCommand(query, connection, transaction)
                        let! affectedRows = command.ExecuteNonQueryAsync()
                        affectedRowsByQuery.Add affectedRows
                    else
                      for parameterSet in parameterSets do
                          use command = new SqlCommand(query, connection, transaction)
                          populateRow command parameterSet
                          let! affectedRows = command.ExecuteNonQueryAsync()
                          affectedRowsByQuery.Add affectedRows
                transaction.Commit()
                return Ok (List.ofSeq affectedRowsByQuery)
            with
            | ex -> return Error ex
        }

    let executeTransactionSafeAsync queries (props: SqlProps)  =
        executeTransactionSafeTask queries props
        |> Async.AwaitTask

    let executeReaderSafe (read: SqlDataReader -> Option<'t>) (props: SqlProps) =
        try
            if Option.isNone props.SqlQuery then failwith "No query provided to execute"
            use connection = new SqlConnection(props.ConnectionString)
            connection.Open()
            use command = new SqlCommand(Option.get props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            populateCmd command props
            use reader = command.ExecuteReader()
            let rows = ResizeArray<'t>()
            while reader.Read() do
                read reader
                |> Option.iter rows.Add
            Ok (List.ofSeq rows)
        with
        | ex -> Error ex

    let executeReaderAsync (read: SqlDataReader -> Option<'t>) (props: SqlProps) : Async<'t list> =
        async {
            if Option.isNone props.SqlQuery then failwith "No query provided to execute"
            use connection = new SqlConnection(props.ConnectionString)
            do! Async.AwaitTask (connection.OpenAsync())
            use command = new SqlCommand(Option.get props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            populateCmd command props
            use! reader = Async.AwaitTask (command.ExecuteReaderAsync())
            let rows = ResizeArray<'t>()
            while reader.Read() do
                reader
                |> read
                |> Option.iter rows.Add
            return List.ofSeq rows
        }

    let executeReaderSafeAsync (read: SqlDataReader -> Option<'t>) (props: SqlProps) =
        async {
            let! result = Async.Catch (executeReaderAsync read props)
            match result with
            | Choice1Of2 value -> return Ok (value)
            | Choice2Of2 err -> return Error (err)
        }

    let executeTable (props: SqlProps) : SqlTable =
        if Option.isNone props.SqlQuery then failwith "No query provided to execute"
        use connection = new SqlConnection(props.ConnectionString)
        connection.Open()
        use command = new SqlCommand(Option.get props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        use reader = command.ExecuteReader()
        readTable reader

    let executeTableSafe (props: SqlProps) : Result<SqlTable, exn> =
        try Ok (executeTable props)
        with | ex -> Error ex

    let executeTableTask (props: SqlProps) =
        task {
            if Option.isNone props.SqlQuery then failwith "No query provided to execute..."
            use connection = new SqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use command = new SqlCommand(Option.get props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
            use! reader = command.ExecuteReaderAsync()
            return! readTableTask reader
        }

    let executeTableAsync (props: SqlProps) =
        executeTableTask props
        |> Async.AwaitTask

    let executeTableSafeTask (props: SqlProps) =
        task {
            try
                if Option.isNone props.SqlQuery then failwith "No query provided to execute"
                use connection = new SqlConnection(props.ConnectionString)
                do! connection.OpenAsync()
                use command = new SqlCommand(Option.get props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
                use! reader = command.ExecuteReaderAsync()
                let! result = readTableTask reader
                return Ok (result)
            with
            | ex -> return Error ex
        }

    let executeTableSafeAsync (props: SqlProps) =
        executeTableSafeTask props
        |> Async.AwaitTask

    let multiline xs = String.concat Environment.NewLine xs

    let executeScalar (props: SqlProps) : SqlValue =
        if Option.isNone props.SqlQuery then failwith "No query provided to execute..."
        use connection = new SqlConnection(props.ConnectionString)
        connection.Open()
        use command = new SqlCommand(Option.get props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        command.ExecuteScalar()
        |> readValue

    let executeNonQuery (props: SqlProps) : int =
        if Option.isNone props.SqlQuery then failwith "No query provided to execute..."
        use connection = new SqlConnection(props.ConnectionString)
        connection.Open()
        use command = new SqlCommand(Option.get props.SqlQuery, connection)
        if props.NeedPrepare then command.Prepare()
        populateCmd command props
        command.ExecuteNonQuery()

    let executeNonQuerySafe (props: SqlProps) : Result<int, exn> =
        try Ok (executeNonQuery props)
        with | ex -> Error ex

    let executeNonQueryTask (props: SqlProps) =
        task {
            use connection = new SqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use command = new SqlCommand(Option.get props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
            return! command.ExecuteNonQueryAsync()
        }

    let executeNonQueryAsync  (props: SqlProps) =
        executeNonQueryTask props
        |> Async.AwaitTask

    let executeNonQuerySafeTask (props: SqlProps) =
        task {
            try
                use connection = new SqlConnection(props.ConnectionString)
                do! connection.OpenAsync()
                use command = new SqlCommand(Option.get props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
                let! result = command.ExecuteNonQueryAsync()
                return Ok (result)
            with
            | ex -> return Error ex
        }

    let executeNonQuerySafeAsync (props: SqlProps) =
        executeNonQuerySafeTask props
        |> Async.AwaitTask

    let executeScalarSafe (props: SqlProps) : Result<SqlValue, exn> =
        try  Ok (executeScalar props)
        with | ex -> Error ex

    let executeScalarTask (props: SqlProps) =
        task {
            if Option.isNone props.SqlQuery then failwith "No query provided to execute..."
            use connection = new SqlConnection(props.ConnectionString)
            do! connection.OpenAsync()
            use command = new SqlCommand(Option.get props.SqlQuery, connection)
            if props.NeedPrepare then command.Prepare()
            do populateCmd command props
            let! value = command.ExecuteScalarAsync()
            return readValue value
        }

    let executeScalarAsync (props: SqlProps) =
        executeScalarTask props
        |> Async.AwaitTask


    let executeScalarSafeTask (props: SqlProps) =
        task {
            try
                if Option.isNone props.SqlQuery then failwith "No query provided to execute..."
                use connection = new SqlConnection(props.ConnectionString)
                do! connection.OpenAsync()
                use command = new SqlCommand(Option.get props.SqlQuery, connection)
                if props.NeedPrepare then command.Prepare()
                do populateCmd command props
                let! value = command.ExecuteScalarAsync()
                return Ok (readValue value)
            with
            | ex -> return Error ex
        }

    let executeScalarSafeAsync (props: SqlProps) =
        executeScalarSafeTask props
        |> Async.AwaitTask

    let mapEachRow (f: SqlRow -> Option<'a>) (table: SqlTable) =
        List.choose f table
