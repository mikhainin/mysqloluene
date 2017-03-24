#include <stdint.h>
#include "mysqld_error.h"   // ER_INVALID_JSON_TEXT
#include "sql_class.h"      // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_mysqloluene.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "log.h"
#include "tnt/iterator.h"
#include "tnt/row.h"
#include "tnt/tuple_builder.h"

static handler *create_handler(handlerton *hton,
                                       TABLE_SHARE *table,
                                       MEM_ROOT *mem_root);

handlerton *example_hton;

/* Interface to mysqld, to check system tables supported by SE */
static const char* example_system_database();
static bool example_is_supported_system_table(const char *db,
                                      const char *table_name,
                                      bool is_sql_layer_system_table);

Mysqloluene_share::Mysqloluene_share()
{
  thr_lock_init(&lock);
}


static int example_init_func(void *p)
{
  DBUG_ENTER("example_init_func");

  example_hton= (handlerton *)p;
  example_hton->state=                     SHOW_OPTION_YES;
  example_hton->create=                    create_handler;
  example_hton->flags=                     HTON_CAN_RECREATE;
  example_hton->system_database=   example_system_database;
  example_hton->is_supported_system_table= example_is_supported_system_table;

  DBUG_RETURN(0);
}


/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each example handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Mysqloluene_share *ha_mysqloluene::get_share()
{
	Mysqloluene_share *tmp_share = nullptr;

  DBUG_ENTER("ha_mysqloluene::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<Mysqloluene_share*>(get_ha_share_ptr())))
  {
    tmp_share= new Mysqloluene_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}


static handler* create_handler(handlerton *hton,
                                       TABLE_SHARE *table,
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_mysqloluene(hton, table);
}

ha_mysqloluene::ha_mysqloluene(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg),
   share(0)
{
	if (table_arg) { // on create table is NULL
		const st_mysql_lex_string &connection = table_arg->connect_string;
		if (!parseConnectionString(std::string(connection.str, connection.length))) {
		 sql_print_warning("Wrong schema'");
		}
	}

}


/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_example_exts[] = {
  NullS
};

const char **ha_mysqloluene::bas_ext() const
{
  return ha_example_exts;
}

/*
  Following handler function provides access to
  system database specific to SE. This interface
  is optional, so every SE need not implement it.
*/
const char* ha_example_system_database= NULL;
const char* example_system_database()
{
  return ha_example_system_database;
}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_handler_tablename ha_example_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval TRUE   Given db.table_name is supported system table.
    @retval FALSE  Given db.table_name is not a supported system table.
*/
static bool example_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_handler_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_example_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}


/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_mysqloluene::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_mysqloluene::open");

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_mysqloluene::close(void)
{
  DBUG_ENTER("ha_mysqloluene::close");
  DBUG_RETURN(0);
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an example of extracting all of the data as strings.
  ha_berekly.cc has an example of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_mysqloluene::write_row(uchar *buf)
{
  DBUG_ENTER("ha_mysqloluene::write_row");

  if (!c.connected()) {
	  c.connect(connection_info.host_port_uri);
	  if (!c.connected()) {
		  DBUG_PRINT("ha_mysqloluene::write_row", ("Not connected, no tarantool connection"));
		  DBUG_RETURN(HA_ERR_NO_CONNECTION);
	  }
  }

  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  tnt::TupleBuilder builder(table->visible_field_count());
  for (Field **field=table->field ; *field ; field++) {

	  	  if ((*field)->is_null()) {
			  builder.pushNull();
	  	  }
		  switch ((*field)->type()) {
			  case MYSQL_TYPE_LONG:
				  builder.push((*field)->val_int());
				  sql_print_warning("write_row: value int( %lld )", (*field)->val_int());
				  break;
			  case MYSQL_TYPE_STRING:
			  case MYSQL_TYPE_VAR_STRING:
			  case MYSQL_TYPE_VARCHAR: {
			  // case MYSQL_TYPE_CHAR:
				  // auto string = r->getString(i);
			      // (*field)->set_notnull();
				  String str;
				  (*field)->val_str(&str);
				  builder.push(str.c_ptr(), str.length());
				  // sql_print_warning("write_row: value str(%s)", str.c_ptr());
				  // (*field)->store(string.c_str(), string.size(), system_charset_info);
				  break;
			  }
			  case MYSQL_TYPE_DATE:
			  case MYSQL_TYPE_DATETIME:
			  case MYSQL_TYPE_TIMESTAMP: {
				  struct timeval tv = {0};
				  int w = 0;
				  (*field)->get_timestamp(&tv, &w); // this function always returns zero
				  builder.push(static_cast<int64_t>(tv.tv_sec));
			  }

		  }
		  //*/
  }

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);

  if (!c.insert(connection_info.space_name, builder)) {
	  DBUG_RETURN(HA_ERR_NO_PARTITION_FOUND);
  }

  DBUG_RETURN(0);
}


/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for example by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_mysqloluene::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_mysqloluene::update_row");

  if (!c.connected()) {
	  c.connect(connection_info.host_port_uri);
	  if (!c.connected()) {
		  DBUG_PRINT("ha_mysqloluene::write_row", ("Not connected, no tarantool connection"));
		  DBUG_RETURN(HA_ERR_NO_CONNECTION);
	  }
  }

  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  tnt::TupleBuilder builder(table->visible_field_count());
  for (Field **field=table->field ; *field ; field++) {

	  	  if ((*field)->is_null()) {
			  builder.pushNull();
	  	  }
		  switch ((*field)->type()) {
			  case MYSQL_TYPE_LONG:
				  builder.push((*field)->val_int());
				  sql_print_warning("write_row: value int( %lld )", (*field)->val_int());
				  break;
			  case MYSQL_TYPE_STRING:
			  case MYSQL_TYPE_VAR_STRING:
			  case MYSQL_TYPE_VARCHAR: {
			  // case MYSQL_TYPE_CHAR:
				  // auto string = r->getString(i);
			      // (*field)->set_notnull();
				  String str;
				  (*field)->val_str(&str);
				  builder.push(str.c_ptr(), str.length());
				  // sql_print_warning("write_row: value str(%s)", str.c_ptr());
				  // (*field)->store(string.c_str(), string.size(), system_charset_info);
				  break;
			  }
			  case MYSQL_TYPE_DATE:
			  case MYSQL_TYPE_DATETIME:
			  case MYSQL_TYPE_TIMESTAMP: {
				  struct timeval tv = {0};
				  int w = 0;
				  (*field)->get_timestamp(&tv, &w); // this function always returns zero
				  builder.push(static_cast<int64_t>(tv.tv_sec));
			  }
		  }
		  //*/
  }

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);

  if (!c.replace(connection_info.space_name, builder)) {
	  DBUG_RETURN(HA_ERR_NO_PARTITION_FOUND);
  }

  DBUG_RETURN(0);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_mysqloluene::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_mysqloluene::delete_row");

  if (!c.connected()) {
	  c.connect(connection_info.host_port_uri);
	  if (!c.connected()) {
		  DBUG_PRINT("ha_mysqloluene::delete_row", ("Not connected, no tarantool connection"));
		  DBUG_RETURN(HA_ERR_NO_CONNECTION);
	  }
  }

  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->read_set);

  tnt::TupleBuilder builder(1); // can remove only by one-field primary key
  for (Field **field=table->field ; *field ; field++) {

	  	  if ((*field)->is_null()) {
			  builder.pushNull();
	  	  }
		  switch ((*field)->type()) {
			  case MYSQL_TYPE_LONG:
				  builder.push((*field)->val_int());
				  goto built; // delete by primary key only (by the first field)
				  // TODO: determine primary key from  Tarantool or table description
				  break;
			  case MYSQL_TYPE_STRING:
			  case MYSQL_TYPE_VAR_STRING:
			  case MYSQL_TYPE_VARCHAR: {
				  String str;
				  (*field)->val_str(&str);
				  builder.push(str.c_ptr(), str.length());
				  goto built; // delete by primary key only (by the first field)
				  // TODO: determine primary key from  Tarantool or table description
				  break;
			  }
			  case MYSQL_TYPE_DATE:
			  case MYSQL_TYPE_DATETIME:
			  case MYSQL_TYPE_TIMESTAMP: {
				  struct timeval tv = {0};
				  int w = 0;
				  (*field)->get_timestamp(&tv, &w); // this function always returns zero
				  builder.push(static_cast<int64_t>(tv.tv_sec));
			  }
		  }
  }
built:
  if (builder.size() == 0) {
	  // TODO: error - no rows to delete
  }

  dbug_tmp_restore_column_map(table->read_set, org_bitmap);

  if (!c.del(connection_info.space_name, builder)) {
	  DBUG_RETURN(HA_ERR_NO_PARTITION_FOUND);
  }

  DBUG_RETURN(0);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/
int ha_mysqloluene::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map,
                               enum ha_rkey_function find_flag MY_ATTRIBUTE((unused)))
{
  int rc = 0;
  DBUG_ENTER("ha_mysqloluene::index_read");
  // MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);

  if (find_flag == HA_READ_KEY_EXACT && keypart_map == 1) { // where id = <value>
	  // TODO: check for key type
	  c.connect(connection_info.host_port_uri);
	  tnt::TupleBuilder builder(1);

	  	  switch (table->key_info[active_index].key_part[0].field->type()) {
			  case MYSQL_TYPE_LONG: {
				  int64_t value = *reinterpret_cast<const int*>(key);
				  builder.push(value);
				  break;
			  }
			  case MYSQL_TYPE_STRING:
			  case MYSQL_TYPE_VAR_STRING:
			  case MYSQL_TYPE_VARCHAR: {
			  // case MYSQL_TYPE_CHAR:

			      uint var_length= uint2korr(key);

			      builder.push(
			    		  reinterpret_cast<const char*>(key+HA_KEY_BLOB_LENGTH),
						  var_length
					  );

				  break;
			  }
			  default:
				  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	  	  }
	  iterator = c.select(connection_info.space_name, builder);
	  table->status = 0;

	  rc = index_next(buf);
  } else {
	  rc= HA_ERR_WRONG_COMMAND;
  }

  // MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_mysqloluene::index_next(uchar *buf)
{
  int rc = 0;
  DBUG_ENTER("ha_mysqloluene::index_next");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  // rc= HA_ERR_WRONG_COMMAND;
  memset((void*)buf, 0, (unsigned long int)table->s->null_bytes);

  auto r = iterator->nextRow();
  if (!r) {
	  rc = HA_ERR_END_OF_FILE;
  } else {
	  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

	  int i = 0;
	  for (Field **field=table->field ; *field ; field++) {
		  if (i < r->getFieldNum()) {

			  if (r->isInt(i)) {
			      (*field)->set_notnull();
				  (*field)->store(r->getInt(i), false);
			  } else if (r->isString(i)) {
				  auto string = r->getString(i);
				  (*field)->set_notnull();
				  (*field)->store(string.c_str(), string.size(), system_charset_info);
			  } else if (r->isNull(i)) {
			      (*field)->set_null();
			      (*field)->reset();
			  } else if (r->isBool(i)) {
				  (*field)->set_notnull();
				  (*field)->store(r->getBool(i), false);
			  } else if (r->isFloatingPoint(i)) {
				  (*field)->set_notnull();
				  (*field)->store(r->getDouble(i));
			  }
		  } else {
		      (*field)->set_null();
		      (*field)->reset();
		  }
		  ++i;
	  }

	  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  }

  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_mysqloluene::index_prev(uchar *buf)
{
  int rc = 0;
  DBUG_ENTER("ha_mysqloluene::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mysqloluene::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mysqloluene::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc = HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_mysqloluene::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_mysqloluene::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the example in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mysqloluene::rnd_init(bool scan)
{
  DBUG_ENTER("ha_mysqloluene::rnd_init");
  if (!c.connected()) {
	  c.connect(connection_info.host_port_uri);
  }
  if (!c.connected()) {
	  DBUG_PRINT("ha_mysqloluene::rnd_init", ("Not connected, no tarantool connection"));
	  DBUG_RETURN(HA_ERR_NO_CONNECTION);
  } else {
	  DBUG_PRINT("ha_mysqloluene::rnd_init", ("Successfully created tarantool connection"));
	  iterator = c.select(connection_info.space_name, tnt::TupleBuilder(0));
	  if (!iterator) {
		  // TODO: set warning
		  DBUG_RETURN(HA_ERR_NO_PARTITION_FOUND);
	  }
	  current_row = 0;
	  DBUG_RETURN(0);
  }
}

int ha_mysqloluene::rnd_end()
{
  DBUG_ENTER("ha_mysqloluene::rnd_end");
  current_row = 0;
  iterator.reset();
  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_mysqloluene::rnd_next(uchar *buf)
{
  int rc = 0;
  DBUG_ENTER("ha_mysqloluene::rnd_next");

  // statistic_increment(table->in_use->status_var.ha_read_rnd_next_count, &LOCK_status);

  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);

  //org_bitmap=
  my_bitmap_map *org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  // auto r = c.select("space"); // TODO:use space from table's connection_string;

  memset((void*)buf, 0, (unsigned long int)table->s->null_bytes);

  auto r = iterator->nextRow();
  if (!r) {
	  rc = HA_ERR_END_OF_FILE;
  } else {
	  int i = 0;
	  for (Field **field=table->field ; *field ; field++) {

		  if (i < r->getFieldNum()) {

			  if (r->isInt(i)) {
			      (*field)->set_notnull();

				  switch ((*field)->type()) {
				  case MYSQL_TYPE_TIMESTAMP:
				  case MYSQL_TYPE_DATE:
				  case MYSQL_TYPE_DATETIME: {
					  struct timeval tm = { r->getInt(i), 0 };
					  (*field)->store_timestamp(&tm);
					  break;
				  }
				  default:
					  (*field)->store(r->getInt(i), false);
				  }
			  } else if (r->isString(i)) {
				  auto string = r->getString(i);
				  (*field)->set_notnull();
				  (*field)->store(string.c_str(), string.size(), system_charset_info);
			  } else if (r->isNull(i)) {
			      (*field)->set_null();
			      (*field)->reset();
			  } else if (r->isBool(i)) {
				  (*field)->set_notnull();
				  (*field)->store(r->getBool(i), false);
			  } else if (r->isFloatingPoint(i)) {
				  (*field)->set_notnull();
				  (*field)->store(r->getDouble(i));
			  }
		  } else {
		      (*field)->set_null();
		      (*field)->reset();
		  }
		  ++i;
	  }
  }
  ++current_row;
  MYSQL_READ_ROW_DONE(rc);

  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  DBUG_RETURN(rc);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_mysqloluene::position(const uchar *record)
{
  DBUG_ENTER("ha_mysqloluene::position");
  my_store_ptr(ref, ref_length, current_row);
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_mysqloluene::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_mysqloluene::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  current_row = my_get_ptr(pos,ref_length);
  rc = current_row;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_mysqloluene::info(uint flag)
{
  DBUG_ENTER("ha_mysqloluene::info");
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_mysqloluene::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_mysqloluene::extra");
  DBUG_PRINT("enter ha_mysqloluene::extra",("function: %d",(int) operation));

  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_mysqloluene::delete_all_rows()
{
  DBUG_ENTER("ha_mysqloluene::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used for handler specific truncate table.  The table is locked in
  exclusive mode and handler is responsible for reseting the auto-
  increment counter.

  @details
  Called from Truncate_statement::handler_truncate.
  Not used if the handlerton supports HTON_CAN_RECREATE, unless this
  engine can be used as a partition. In this case, it is invoked when
  a particular partition is to be truncated.

  @see
  Truncate_statement in sql_truncate.cc
  Remarks in handler::truncate.
*/
int ha_mysqloluene::truncate()
{
  DBUG_ENTER("ha_mysqloluene::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_mysqloluene::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_mysqloluene::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_mysqloluene::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_mysqloluene::delete_table(const char *name)
{
  DBUG_ENTER("ha_mysqloluene::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}


/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_mysqloluene::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_mysqloluene::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_mysqloluene::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_mysqloluene::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_mysqloluene::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_mysqloluene::create");
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */
  DBUG_RETURN(0);
}

// TODO: rewrite using Boost.Spirit
bool ha_mysqloluene::parseConnectionString(const std::string &connection_string)
{
 // tnt://localhost:3301/isp
 // tnt://localhost:3301/:513
 if (strncmp(connection_string.data(), "tnt://", 6) != 0) {
	 sql_print_warning("Wrong schema'");
	 return false;
 }

 size_t colon_point = connection_string.find(':', 6);
 if (colon_point == std::string::npos) {
	 sql_print_warning("Can't find colon_point'");
	 return false;
 }

 const std::string &hostname = connection_string.substr(6, colon_point - 6);
 connection_info.hostname = hostname;
 sql_print_warning("hostname: %s", hostname.c_str());

 colon_point++;
 size_t slash_point = connection_string.find('/', colon_point);
 if (slash_point == std::string::npos) {
	 sql_print_warning("Can't find slash_point'");
	 return false;
 }
 const std::string &port_string = connection_string.substr(colon_point, slash_point - colon_point);
 connection_info.port = atoi(port_string.c_str());
 sql_print_warning("port: %d (%s)", connection_info.port, port_string.c_str());

 connection_info.host_port_uri = connection_string.substr(6, slash_point - 6);
 sql_print_warning("host:port: '%s'", connection_info.host_port_uri.c_str());

 const std::string &space = connection_string.substr(slash_point + 1);
 if (space.empty()) {
	 sql_print_warning("Space is empty'");
	 return false;
 }
 sql_print_warning("space: %s", space.c_str());

 if (space[0] == ':') {
	 connection_info.space_id = atoi(space.c_str());
	 sql_print_warning("space id: %d", connection_info.space_id);
 } else {
	 connection_info.space_name = space;
	 sql_print_warning("space name: %s", connection_info.space_name.c_str());
 }
 return true;
}

struct st_mysql_storage_engine mysqloulene_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  double_var,
  srv_double_var,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);                             // reserved always 0

static MYSQL_THDVAR_DOUBLE(
  double_thdvar,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);

static struct st_mysql_sys_var* example_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  MYSQL_SYSVAR(double_var),
  MYSQL_SYSVAR(double_thdvar),
  NULL
};

// this is an example of SHOW_FUNC and of my_snprintf() service
static int show_func_example(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, srv_double_var, "really");
  return 0;
}

struct example_vars_t
{
	ulong  var1;
	double var2;
	char   var3[64];
  bool   var4;
  bool   var5;
  ulong  var6;
};

example_vars_t example_vars= {100, 20.01, "three hundred", true, 0, 8250};

static st_mysql_show_var show_status_example[]=
{
  {"var1", (char *)&example_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
  {"var2", (char *)&example_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF} // null terminator required
};

static struct st_mysql_show_var show_array_example[]=
{
  {"array", (char *)show_status_example, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
  {"var3", (char *)&example_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
  {"var4", (char *)&example_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF}
};

static struct st_mysql_show_var func_status[]=
{
  {"example_func_example", (char *)show_func_example, SHOW_FUNC, SHOW_SCOPE_GLOBAL},
  {"example_status_var5", (char *)&example_vars.var5, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
  {"example_status_var6", (char *)&example_vars.var6, SHOW_LONG, SHOW_SCOPE_GLOBAL},
  {"example_status",  (char *)show_array_example, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
  {0,0,SHOW_UNDEF, SHOW_SCOPE_UNDEF}
};

mysql_declare_plugin(mysqloluene)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &mysqloulene_storage_engine,
  "TARANTOOL",
  "Mikhail Galanin",
  "Tarantool storage engine",
  PLUGIN_LICENSE_BSD,
  example_init_func,                            /* Plugin Init */
  NULL,                                         /* Plugin Deinit */
  0x0001 /* 0.1 */,
  func_status,                                  /* stat	us variables */
  example_system_variables,                     /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
