#!/usr/bin/env sysbench
-- This test is designed for testing MariaDB's key_cache_segments for MyISAM,
-- and should work with other storage engines as well.
--
-- For details about key_cache_segments please refer to:
-- http://kb.askmonty.org/v/segmented-key-cache
--

-- require("oltp_common")
-- Override standard prepare/cleanup OLTP functions, as this benchmark does not
-- -- support multiple tables
-- oltp_prepare = prepare
-- oltp_cleanup = cleanup

sysbench.cmdline.options = {
   table_size =
      {"Number of rows per table", 10000},
   tables =
      {"Number of tables", 1},
   non_index_updates =
      {"Number of UPDATE non-index queries per transaction", 1}
}

str_index = 1
cur_index = 1
step = 2500
block_num = 1

local c_value_template_2k1 = "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################"

local c_value_template_2k2 = "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################" ..
   "####################################################################################################"

function create_table_mhy(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query
   id_def = "bigint(20) unsigned NOT NULL AUTO_INCREMENT"
   
   engine_def = "engine = InnoDB"
   extra_table_options = "CHARSET=utf8mb4 ROW_FORMAT=COMPACT"

   print(string.format("Creating table 't_block_data_%d'...", table_num))

   query = string.format([[
CREATE TABLE `t_block_record_pk_%d`(
  `uid` int(10) NOT NULL DEFAULT '0',
  `block_id` int(10) NOT NULL DEFAULT '0',
  `data_version` int(10) NOT NULL,
  `bin_data` mediumblob NOT NULL,
  `last_save_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY `UID_BLOCK_ID` (`uid`,`block_id`) USING BTREE
) %s %s]],
      table_num, engine_def, extra_table_options)

   con:query(query)

   print(sysbench.opt.table_size)
   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into 't_block_data_%d'", sysbench.opt.table_size, table_num))
   end

   query = "INSERT INTO t_block_record_pk_" .. table_num .. "(uid, block_id, data_version, bin_data) VALUES"
   
   con:bulk_insert_init(query)

   for i = 1, sysbench.opt.table_size do
      for j = 1, block_num do
         user_id_val = i 
         block_id_val = j
         data_version_val = 2274 
         bin_data_val = sysbench.rand.string(c_value_template_2k1)

         query = string.format("(%d, '%d', '%d', '%s')", user_id_val, block_id_val, data_version_val, bin_data_val)

         con:bulk_insert_next(query)
      end
   end
   con:bulk_insert_done()
end

function cmd_prepare()
   print(string.format("Preparing table 't_block_data'"))
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables, sysbench.opt.threads do
     create_table_mhy(drv, con, i)
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
      print(string.format("Dropping table 't_block_record_pk_%d'...", i))
      con:query("DROP TABLE IF EXISTS t_block_record_pk_" .. i )
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
   non_index_updates = {
      "UPDATE t_block_record_pk_%u SET bin_data =?,  data_version = 2274 WHERE uid=? and block_id=? and data_version <= 2274 limit 1",
      {t.CHAR, 204800}, t.INT, t.INT}
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


function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   stmt = {}
   param = {}

   for t = 1, sysbench.opt.tables do
      stmt[t] = {}
      param[t] = {}
   end
   prepare_for_each_table("non_index_updates")

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
   -- To prevent overlapping of our range queries we need to partition the whole
   -- table into 'threads' segments and then make each thread work with its
   -- own segment.
      --local rmin = rlen * thread_id
      --local rmax = rmin + rlen

      -- local rmin = 1
       --local rmin = 1230643
      -- local rmax = 12821
      -- local rmax = 8919883
      --params[1]:set(sb_rand(rmin, rmax))

   local tnum = get_table_num()

   for i = 1, sysbench.opt.non_index_updates do
      user_id = sysbench.rand.default(1, sysbench.opt.table_size)
      block_id = sysbench.rand.default(1, block_num)

      if math.random(0,100) % 10 < 5 then
          param[tnum].non_index_updates[1]:set_rand_str(c_value_template_2k1)
      else
          param[tnum].non_index_updates[1]:set_rand_str(c_value_template_2k2)
      end
      param[tnum].non_index_updates[2]:set(user_id)
      param[tnum].non_index_updates[3]:set(block_id)

      stmt[tnum].non_index_updates:execute()
   end

end
