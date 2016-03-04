PROJECT = texas_sqlite

DEPS = esqlite lager
dep_esqlite = git https://github.com/mmzeeman/esqlite.git master
dep_lager = git https://github.com/basho/lager.git master

include erlang.mk

