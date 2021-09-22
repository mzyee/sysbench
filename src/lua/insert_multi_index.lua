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

require("oltp_common")

sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}
}

prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}

function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   if (sysbench.opt.tables == 1) then
      for i = 1, sysbench.opt.threads, 1 do
        create_table(drv, con, 1)
      end
   else
      for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables, sysbench.opt.threads do
        print("thread id: " .. i)
        create_table(drv, con, i)
      end
   end
end

function prepare_statements()
end

function create_table(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query
   id_def = "bigint(20) unsigned NOT NULL AUTO_INCREMENT"

   engine_def = "engine = InnoDB"
   extra_table_options = "CHARSET=utf8 COLLATE=utf8_bin"


   print(string.format("Creating table 'sbtest%d'", table_num))

   query = string.format("                          \
   CREATE TABLE IF NOT EXISTS `sbtest%d` (                         \
  `id` bigint(20) NOT NULL, \
  `k` int(11) NOT NULL DEFAULT '0',                 \
  `k2` bigint(20) unsigned NOT NULL DEFAULT '0',    \
  `k3` bigint(20) unsigned NOT NULL DEFAULT '0',    \
  `c` char(120) NOT NULL DEFAULT '',  \
  `pad` char(60) NOT NULL DEFAULT '', \
  PRIMARY KEY (`id`), \
  KEY `p_1` (`pad`),  \
  KEY `p_2` (`pad`),  \
  KEY `p_3` (`pad`),  \
  KEY `p_4` (`pad`),  \
  KEY `p_5` (`pad`),  \
  KEY `p_6` (`pad`),  \
  KEY `p_7` (`pad`),  \
  KEY `p_8` (`pad`),  \
  KEY `p_9` (`pad`),  \
  KEY `p_10` (`pad`)) \
 ENGINE=InnoDB\n", table_num)
   con:query(query)
   query = string.format("insert into sbtest%d(`id`,  `k`, `k2`, `k3`,`c`,`pad` ) values ", table_num)

   con:bulk_insert_init(query)

   print(string.format("insert table 'sbtest%d' %d rows", table_num, sysbench.opt.table_size))
   for i = 1, sysbench.opt.table_size do
     c_val = get_c_value()
     pad_val = get_pad_value()
     query = string.format("(%d, %d, %d, %d, '%s', '%s' )", 0, i, i, i, c_val,pad_val )
     con:bulk_insert_next(query)
   end
   con:bulk_insert_done()
end

local function get_table_num()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping table 'sbtest%d'...", i))
      con:query(string.format("DROP TABLE IF EXISTS sbtest%d", i))
   end
end

function event()
   local table_name = "sbtest1"
   local k_val = sysbench.rand.default(1, sysbench.opt.table_size)
   local c_val = get_c_value()
   local pad_val = get_pad_value()

   local tnum = sysbench.tid % sysbench.opt.tables + 1

   if (drv:name() == "pgsql" and sysbench.opt.auto_inc) then
      con:query(string.format("INSERT INTO %s (k, c, pad) VALUES " ..
                                 "(%d, '%s', '%s')",
                              table_name, k_val, c_val, pad_val))
   else
      if (sysbench.opt.auto_inc) then
         i = 0
      else
         i = sysbench.rand.unique() - 2147483648
      end

      i = sysbench.rand.uniform(1, 100000000)

      id = sysbench.rand.unique() - 2147483648
      query = string.format("insert into sbtest%d(`id`,  `k`, `k2`, `k3`,`c`,`pad` ) values (%d, %d, %d, %d, '%s', '%s' )", tnum, id, i, i, i, c_val,pad_val)
      con:query(query)
   end
end
