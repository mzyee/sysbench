#!/usr/bin/env sysbench

-- require("oltp_common")
-- Override standard prepare/cleanup OLTP functions, as this benchmark does not
-- support multiple tables
-- oltp_prepare = prepare
-- oltp_cleanup = cleanup

sysbench.cmdline.options = {
   table_size =
      {"Number of rows per table", 10000},
   tables =
      {"Number of tables", 1},
   line_insert =
      {"Number of insert queries per transaction", 1},
   skip_trx =
      {"skip trx", false},
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"}
}

local aid_value_template = "##########-##########-##########"
local data='0123456789'

function get_aid_value()
   return sysbench.rand.string(aid_value_template)
end
  
function create_table(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query
   id_def = "bigint(20) unsigned NOT NULL AUTO_INCREMENT"
   
   engine_def = "engine = InnoDB"
   extra_table_options = "CHARSET=utf8 COLLATE=utf8_bin"

   print(string.format("Creating table 'small_table_%d'...", table_num))

   query = string.format([[
CREATE TABLE IF NOT EXISTS`small_table_%d` (
  `id` %s, 
  `aid` char(38) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `type` varchar(45) NOT NULL DEFAULT '1',
  `data` varchar(1024) NOT NULL,
  `state` smallint DEFAULT '0' COMMENT '0 : AVALIABLE, 1 DELETED',
  `createTS` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(id),
  KEY `live_item_idx` (`aid`)
) %s ]],
      table_num, id_def, engine_def)

   con:query(query)

   query = string.format("insert ignore small_table_%s(`aid`,  `type`, `data`, `state` ) values ", table_num)

   con:bulk_insert_init(query)


   for i = 1, sysbench.opt.table_size do
      query = string.format("('%s', '', '%s',  0 )", get_aid_value(), data)
      con:bulk_insert_next(query)
   end

   con:bulk_insert_done()
end

function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables, sysbench.opt.threads do
      print(string.format("Inserting records into 'small_table_%d'", i))
      create_table(drv, con, i)
   end

end

-- Implement parallel prepare and prewarm commands
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}
}

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping table 'small_table_%d'...", i))
      con:query("DROP TABLE IF EXISTS small_table_" .. i )
   end
end

local function get_table_num()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
end

local function get_id()
   return sysbench.rand.default(1, sysbench.opt.table_size)
end

local t = sysbench.sql.type
local stmt_defs = {
   line_insert = {
      "insert into small_table_%s(`aid`,  `type`, `data`, `state` ) values ( ?, '', ?,  0 )", 
      {t.CHAR, 1024}, {t.CHAR, 1024}}
}

function prepare_for_each_table(key)
   for t = 1, sysbench.opt.tables do
      stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], t))

      local nparam = #stmt_defs[key] - 1

      if nparam > 0 then
         param[t][key] = {}
      end

      for p = 1, nparam do
         local btype = stmt_defs[key][p+1]
         local len

         if type(btype) == "table" then
            len = btype[2]
            btype = btype[1]
         end
         if btype == sysbench.sql.type.VARCHAR or
            btype == sysbench.sql.type.CHAR then
               param[t][key][p] = stmt[t][key]:bind_create(btype, len)
         else
            param[t][key][p] = stmt[t][key]:bind_create(btype)
         end
      end

      if nparam > 0 then
         stmt[t][key]:bind_param(unpack(param[t][key]))
      end
   end
end


function execute_line_insert()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.line_insert do
      param[tnum].line_insert[1]:set_rand_str(aid_value_template)
      param[tnum].line_insert[2]:set_rand_str(aid_value_template)

      stmt[tnum].line_insert:execute()
   end
end

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   stmt = {}
   param = {}

   for t = 1, sysbench.opt.tables do
      stmt[t] = {}
      param[t] = {}
   end
   prepare_for_each_table("line_insert")

   rlen = sysbench.opt.table_size / sysbench.opt.threads

   thread_id = sysbench.tid % sysbench.opt.threads
end

function thread_done()
   for t = 1, sysbench.opt.tables do
      for k, s in pairs(stmt[t]) do
         stmt[t][k]:close()
      end
   end
   con:disconnect()
end



function event()
   -- local table_name = "sbtest" .. sysbench.rand.uniform(1, sysbench.opt.tables)
   -- local k_val = sysbench.rand.default(1, sysbench.opt.table_size)
   -- local c_val = get_c_value()
   --local c_val = sysbench.rand.string(aid_value_template)

   if (sysbench.opt.auto_inc) then
      i = 0
   else
      i = sysbench.rand.unique() - 2147483648
   end

   execute_line_insert()
end
