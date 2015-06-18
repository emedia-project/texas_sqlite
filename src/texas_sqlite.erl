-module(texas_sqlite).

-export([start/0]).
-export([connect/6, exec/2, close/1]).
-export([create_table/2, create_table/3, drop_table/2]).
-export([insert/3, select/4, count/3, update/4, delete/3]).
-export([string_separator/0, string_quote/0, field_separator/0]).
-export([where_null/2, set_null/1]).

-define(STRING_SEPARATOR, $').
-define(STRING_QUOTE, $').
-define(FIELD_SEPARATOR, none).

-type connection() :: any().
-type err() :: any().
-type tablename() :: atom().
-type data() :: any().
-type clause_type() :: where | group | order | limit.
-type clause() :: {clause_type(), string(), [tuple()]} |
                  {clause_type(), string(), []}.
-type clauses() :: [clause()] | [].

string_separator() -> ?STRING_SEPARATOR.
string_quote()     -> ?STRING_QUOTE.
field_separator()  -> ?FIELD_SEPARATOR.
where_null("=", Key)  -> io_lib:format("~s IS NULL", [texas_sql:sql_field(Key, ?MODULE)]);
where_null(_, Key)  -> io_lib:format("~s IS NOT NULL", [texas_sql:sql_field(Key, ?MODULE)]).
set_null(Key)  -> io_lib:format("~s = NULL", [texas_sql:sql_field(Key, ?MODULE)]).

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
  esqlite3:exec("commit;", texas:connection(Conn)),
  case esqlite3:close(texas:connection(Conn)) of
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
              Table:'-type'(Field),
              Table:'-autoincrement'(Field),
              Table:'-not_null'(Field),
              Table:'-unique'(Field),
              Table:'-default'(Field))
        end, Table:'-fields'())),
  lager:debug("~s", [SQLCmd]),
  exec(SQLCmd, Conn).

-spec create_table(connection(), tablename(), list()) -> ok | error.
create_table(Conn, Table, Fields) ->
  SQLCmd = sql(
      create_table,
      atom_to_list(Table),
      lists:map(fun({Field, Options}) ->
            sql(
              column_def,
              atom_to_list(Field),
              texas_sql:get_option(type, Options),
              texas_sql:get_option(autoincrement, Options),
              texas_sql:get_option(not_null, Options),
              texas_sql:get_option(unique, Options),
              texas_sql:get_option(default, Options))
        end, Fields)),
  lager:debug("~s", [SQLCmd]),
  exec(SQLCmd, Conn).

-spec drop_table(connection(), tablename()) -> ok | error.
drop_table(Conn, Table) ->
  SQLCmd = "DROP TABLE IF EXISTS " ++ atom_to_list(Table),
  lager:debug("~s", [SQLCmd]),
  exec(SQLCmd, Conn).

-spec insert(connection(), tablename(), data() | list()) -> data() | ok | {error, err()}.
insert(Conn, Table, Record) ->
  SQLCmd = "INSERT INTO " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:insert_clause(Record, ?MODULE),
  lager:debug("~s", [SQLCmd]),
  case esqlite3:insert(SQLCmd, texas:connection(Conn)) of
    {ok, ID} ->
      case texas_sql:defined_table(Table) of
        true ->
          case Table:'-table_pk_id'() of
            {none, null} -> Record;
            {ok, Col} -> select(Conn, Table, first, [{where, [{Col, ID}]}])
          end;
        false -> ok
      end;
    E -> E
  end.

-spec select(connection(), tablename(), first | all, clauses()) ->
  data() | [data()] | [] | {error, err()}.
select(Conn, Table, Type, Clauses) ->
  SQLCmd = "SELECT * FROM " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:where_clause(texas_sql:clause(where, Clauses), ?MODULE),
           % TODO : add GROUP BY, ORDER BY, LIMIT
  lager:debug("~s", [SQLCmd]),
  Assoc = fun(Names, Row) ->
      lists:zip(tuple_to_list(Names), tuple_to_list(Row))
  end,
  case Type of
    first ->
      {ok, Statement} = esqlite3:prepare(SQLCmd, texas:connection(Conn)),
      case esqlite3:fetchone(Statement) of
        ok -> [];
        Row -> case texas_sql:defined_table(Table) of
            true -> Table:new(Conn, Assoc(esqlite3:column_names(Statement), Row));
            _ -> Assoc(esqlite3:column_names(Statement), Row)
          end
      end;
    _ ->
      case esqlite3:map(Assoc, SQLCmd, texas:connection(Conn)) of
        [] -> [];
        Data -> lists:map(fun(D) ->
                case texas_sql:defined_table(Table) of
                  true -> Table:new(Conn, D);
                  _ -> D
                end
            end, Data)
      end
  end.

-spec count(connection(), tablename(), clauses()) -> integer().
count(Conn, Table, Clauses) ->
  SQLCmd = "SELECT COUNT(*) FROM " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:where_clause(texas_sql:clause(where, Clauses), ?MODULE),
           case esqlite3:q(SQLCmd, [], texas:connection(Conn)) of
    [{N}] -> N;
    _ -> 0
  end.

-spec update(connection(), tablename(), data(), [tuple()]) -> [data()] | {error, err()}.
update(Conn, Table, Record, UpdateData) ->
  SQLCmd = "UPDATE " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:set_clause(UpdateData, ?MODULE) ++
           texas_sql:where_clause(Record, ?MODULE),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    ok ->
      UpdateRecord = lists:foldl(fun({Field, Value}, Rec) ->
              Rec:Field(Value)
          end, Record, UpdateData),
      select(Conn, Table, all, [{where, UpdateRecord}]);
    error -> {error, update_error}
  end.

-spec delete(connection(), tablename(), data()) -> ok | {error, err()}.
delete(Conn, Table, Record) ->
  SQLCmd = "DELETE FROM " ++
           texas_sql:sql_field(Table, ?MODULE) ++
           texas_sql:where_clause(Record, ?MODULE),
  lager:debug("~s", [SQLCmd]),
  case exec(SQLCmd, Conn) of
    ok -> ok;
    error -> {error, delete_error}
  end.

% Private --

exec(SQL, Conn) ->
  case esqlite3:exec(SQL, texas:connection(Conn)) of
    {error, _} -> error;
    _ -> ok
  end.

sql(create_table, Name, ColDefs) ->
  "CREATE TABLE IF NOT EXISTS " ++ Name ++ " (" ++ string:join(ColDefs, ", ") ++ ");";
sql(default, date, {ok, now}) -> " DEFAULT CURRENT_DATE";
sql(default, time, {ok, now}) -> " DEFAULT CURRENT_TIME";
sql(default, datetime, {ok, now}) -> " DEFAULT CURRENT_TIMESTAMP";
sql(default, _, {ok, Value}) -> io_lib:format(" DEFAULT ~s", [texas_sql:sql_string(Value, ?MODULE)]);
sql(default, _, _) -> "".
sql(column_def, Name, Type, Autoincrement, NotNull, Unique, Default) ->
  Name ++
  sql(type, Type) ++
  sql(autoinc, Autoincrement) ++
  sql(notnull, NotNull) ++
  sql(unique, Unique) ++
  sql(default, Type, Default).
sql(type, id) -> " INTEGER";
sql(type, integer) -> " INTEGER";
sql(type, string) -> " TEXT";
sql(type, float) -> " REAL";
sql(type, _) -> " TEXT";
sql(autoinc, {ok, true}) -> " PRIMARY KEY AUTOINCREMENT";
sql(notnull, {ok, true}) -> " NOT NULL";
sql(unique, {ok, true}) -> " UNIQUE";
sql(_, _) -> "".
