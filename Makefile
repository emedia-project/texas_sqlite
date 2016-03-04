PROJECT = texas_sqlite

DEPS = esqlite lager texas_adapter
dep_esqlite = git https://github.com/mmzeeman/esqlite.git master
dep_lager = git https://github.com/basho/lager.git master
dep_texas_adapter = git https://github.com/emedia-project/texas_adapter.git master

include erlang.mk

