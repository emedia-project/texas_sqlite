-module(texas_sqlite).

-export([start/0]).
-export([connect/6, exec/2, close/1]).
-export([create_table/2]).
-export([insert/3, select/4, update/4, delete/3]).

-define(STRING_SEPARATOR, $').
-define(STRING_QUOTE, $').

-type connection() :: any().
-type err() :: any().
-type tablename() :: atom().
-type data() :: any().
-type clause_type() :: where | group | order | limit.
-type clause() :: {clause_type(), string(), [tuple()]} |
                  {clause_type(), string(), []}.
-type clauses() :: [clause()] | [].

-spec start() -> ok.
start() ->
  ok.

-spec connect(string(), string(), string(), integer(), string(), any()) -> 
  {ok, connection()} | {error, err()}.
connect(_User, _Password, _Server, _Port, Database, _Options) ->
  lager:debug("Open database ~p", [Database]),
  esqlite3:open(Database).

-spec close(connection()) -> ok | error.
close(Conn) ->
  esqlite3:exec("commit;", Conn),
  case esqlite3:close(Conn) of
    ok -> ok;
    {error, _} -> error
  end.

-spec create_table(connection(), tablename()) -> ok | error.
create_table(Conn, Table) ->
  SQLCmd = sql(
    create_table, 
    atom_to_list(Table), 
    lists:map(fun(Field) ->
            sql(
              column_def, 
              atom_to_list(Field),
              Table:type(Field),
              Table:autoincrement(Field),
              Table:not_null(Field),
              Table:unique(Field),
              Table:default(Field))
        end, Table:fields())),
  lager:debug("~s", [SQLCmd]),
  exec(SQLCmd, Conn).

-spec insert(connection(), tablename(), data()) -> data() | {error, err()}.
insert(Conn, Table, Record) ->
  {Fields, Values} = lists:foldl(fun(Field, {FieldsAcc, ValuesAcc}) ->
          case Record:Field() of
            undefined -> {FieldsAcc, ValuesAcc};
            Value -> {FieldsAcc ++ [atom_to_list(Field)], 
                      ValuesAcc ++ [texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE)]}
          end
      end, {[], []}, Table:fields()),
  SQLCmd = sql(insert, atom_to_list(Table), Fields, Values),
  lager:debug("~s", [SQLCmd]),
  case esqlite3:insert(SQLCmd, Conn) of
    {ok, ID} -> 
      case Table:table_pk_id() of
        {none, null} -> Record;
        {ok, Col} -> select(Conn, Table, first, [{where, io_lib:format("~p = :id", [Col]), [{id, ID}]}])
      end;
    E -> E
  end.

-spec select(connection(), tablename(), first | all, clauses()) -> 
  data() | [data()] | [] | {error, err()}.
select(Conn, Table, Type, Clauses) -> 
  SQLCmd = sql(select, atom_to_list(Table), sql(clause, Clauses)),
  Assoc = fun(Names, Row) -> 
      lists:zip(tuple_to_list(Names), tuple_to_list(Row))
  end,
  lager:debug("~s", [SQLCmd]),
  case Type of
    first ->
      {ok, Statement} = esqlite3:prepare(SQLCmd, Conn),
      case esqlite3:fetchone(Statement) of
        ok -> [];
        Row -> Table:new(Assoc(esqlite3:column_names(Statement), Row))
      end;
    _ ->
      case esqlite3:map(Assoc, SQLCmd, Conn) of
        [] -> [];
        Data -> lists:map(fun(D) -> Table:new(D) end, Data)
      end
  end.

-spec update(connection(), tablename(), data(), [tuple()]) -> [data()] | {error, err()}.
update(Conn, Table, Record, UpdateData) ->
  Where = join(lists:foldl(fun(Field, W) ->
            case Record:Field() of
              undefined -> W;
              Value -> W ++ [{Field, Value}]
            end
        end, [], Table:fields()), " AND "),
  Set = join(UpdateData, ", "),
  SQLCmd = "UPDATE " ++ atom_to_list(Table) ++ " SET " ++ Set ++ " WHERE " ++ Where ++ ";",
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    ok -> 
      UpdateRecord = lists:foldl(fun({Field, Value}, Rec) ->
              Rec:Field(Value)
          end, Record, UpdateData),
      select(Conn, Table, all, [texas_sql:record_to_where_clause(Table, UpdateRecord)]);
    error -> {error, update_error}
  end.

-spec delete(connection(), tablename(), data()) -> ok | {error, err()}.
delete(Conn, Table, Record) ->
  WhereClause = texas_sql:record_to_where_clause(Table, Record),
  SQLCmd = sql(delete, atom_to_list(Table), sql(clause, [WhereClause])),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    ok -> ok;
    error -> {error, delete_error}
  end.

% Private --

exec(SQL, Conn) ->
  case esqlite3:exec(SQL, Conn) of
    {error, _} -> error;
    _ -> ok
  end.

join(KVList, Sep) ->
  string:join(lists:map(fun({K, V}) ->
          io_lib:format("~p = ~p", [K, texas_sql:to_sql_string(V, ?STRING_SEPARATOR, ?STRING_QUOTE)])
      end, KVList), Sep).

sql(create_table, Name, ColDefs) -> 
  "CREATE TABLE IF NOT EXISTS " ++ Name ++ " (" ++ string:join(ColDefs, ", ") ++ ");";
sql(select, Name, Clauses) ->
  "SELECT * FROM " ++ Name ++ " " ++ string:join(Clauses, " ") ++ ";";
sql(delete, Name, Clauses) ->
  "DELETE FROM " ++ Name ++ " " ++ string:join(Clauses, " ") ++ ";".
sql(insert, Table, Fields, Values) ->
  "INSERT INTO " ++ Table ++ "(" ++ string:join(Fields, ", ") ++ ") VALUES (" ++ string:join(Values, ", ") ++ ");".
sql(column_def, Name, Type, Autoincrement, NotNull, Unique, Default) ->
  Name ++ 
  sql(type, Type) ++ 
  sql(autoinc, Autoincrement) ++ 
  sql(notnull, NotNull) ++
  sql(unique, Unique) ++
  sql(default, Default).
sql(where, Data) -> "WHERE " ++ Data;
sql(group, Data) -> "GROUP BY " ++ Data;
sql(order, Data) -> "ORDER BY " ++ Data;
sql(limit, Data) -> "LIMIT " ++ Data;
sql(type, id) -> " INTEGER";
sql(type, integer) -> " INTEGER";
sql(type, string) -> " TEXT";
sql(type, float) -> " REAL";
sql(type, _) -> " TEXT";
sql(autoinc, {ok, true}) -> " PRIMARY KEY AUTOINCREMENT";
sql(notnull, {ok, true}) -> " NOT NULL";
sql(unique, {ok, true}) -> " UNIQUE";
sql(default, {ok, Value}) -> io_lib:format(" DEFAULT ~p", [texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE)]);
sql(clause, Clauses) when is_list(Clauses) ->
  lists:map(fun(Clause) -> sql(clause, Clause) end, Clauses);
sql(clause, {Type, Str, Params}) ->
  WhereClause = lists:foldl(fun({Field, Value}, Clause) ->
        estring:gsub(Clause, ":" ++ atom_to_list(Field), texas_sql:to_sql_string(Value, ?STRING_SEPARATOR, ?STRING_QUOTE))
    end, Str, Params),
  sql(Type, WhereClause);
sql(clause, {Type, Str}) ->
  sql(clause, {Type, Str, []});
sql(_, _) -> "".
