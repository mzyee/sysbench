#!/usr/bin/env sysbench
-- Copyright (C) 2006-2017 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- ----------------------------------------------------------------------
-- Insert-Only OLTP benchmark
-- ----------------------------------------------------------------------

require("oltp_common")

sysbench.cmdline.commands.prepare = {
   function ()
      if (not sysbench.opt.auto_inc) then
         sysbench.opt.table_size=0
      end

      cmd_prepare()
   end,
   sysbench.cmdline.PARALLEL_COMMAND
}

function prepare_statements()
end

function event()
   local table_name = "sbtest1"
   local k_val = sysbench.rand.default(1, sysbench.opt.table_size)
   local rang_size = 0.001 * sysbench.opt.table_size
   local c_val = get_c_value()
   local pad_val = get_pad_value()

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
     
      id = sysbench.rand.default(1, rang_size)
      con:query(string.format("UPDATE %s set k=k+1 WHERE id = %s ", table_name, id))
   end
end
