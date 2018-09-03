from zipline.data.bundles import register
from zipline.data.bundles.sqlitedb import sqlitedb_equities

equities = {'SH000001','SZ300079'}

register(
   'sqlitedb',  # name this whatever you like
    sqlitedb_equities(equities),
    calendar_name='SHSZ'
)
