#-*- coding: utf-8 -*-
# author:zhaolun
# the script help managing partitioned table by reading `table.conf`, it should be run with crond.
import os,sys,commands,re,ConfigParser
from datetime import datetime, timedelta
SERVER_CONNENT = "mysql -h%(HOSTIP)s -P%(PORT)s -u%(USER)s -p%(PASSWORD)s -D%(DATABASE)s --default-character-set=utf8 -N -e '%(SQL)s' "
SQL_TRUNCATE   = "alter table %s truncate partition %s"
SQL_DROP       = "alter table %s drop partition %s"
SQL_ADD        = "alter table %s add partition (%s)"
list_param = ["HOSTIP","PORT","USER","PASSWORD","DATABASE","TABLES","KEEP_DAY","TYPE","ACTION"]

DEBUG_MODE = True
def getCurTime():
	return datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')

def excuteSql(sectionMap,excute_sql):
	sectionMap = sectionMap.copy()
	sectionMap["SQL"] = excute_sql
	command_execute = SERVER_CONNENT % sectionMap
	#print getCurTime(), command_execute
	status,output = commands.getstatusoutput(command_execute)
	if status != 0:
		print '[ERROR]',output
	return status,output.replace('Warning: Using a password on the command line interface can be insecure.','')
def getAllPartitions(map_param):

	status, output = excuteSql(map_param,"show create table " + map_param['TABLE'])
	if status != 0:
		print getCurTime(), "[ERROR] get partiton faild: msg=>",output
		return None
	#print getCurTime(), "SUCESS " + output
	m = re.compile(r'PARTITION p_(\d+) VALUES LESS THAN \(\d+\)')
	partitions = []
	for line in output.split(r'\n'):
		r = m.search(line)
		if r is not None:
			partitions.append(r.group(1))
	return partitions


def checkAndGetSectionMap(configParser,section):
	map_param = {}
	for param in list_param:
		tmp_str = configParser.get(section, param)
		if tmp_str == '' or tmp_str is None:
			print getCurTime(), "=>",section,param,"value is :",tmp_str
			return None
		else:
			map_param[param] = tmp_str
			if param == "TYPE":
				tmp_str = tmp_str.upper()
				#print getCurTime(), tmp_str
				if tmp_str not in ["DAY","HOUR"]:
					print getCurTime(), "=>",section,"TYPE ERROR:",tmp_str
					return None
				map_param[param] = tmp_str
			elif param  == 'KEEP_DAY':
				if tmp_str.isdigit():
					map_param[param] = int(tmp_str)
				else:
					print getCurTime(), "=>",section,"KEEP_DAY ERROR:",tmp_str
					return None
			elif param == "ACTION":
				real_actions = []
				actions = tmp_str.upper().split(',')
				for action in actions:
					action = action.strip()
					if action not in ['ADD','DROP','TRUNCATE']:
						print getCurTime(), "=>",section,"ACTION ERROR:",tmp_str
						return None 
					if action not in real_actions:
						real_actions.append(action)
				if 'TRUNCATE' in real_actions:
					real_actions.remove('TRUNCATE')
					real_actions.insert(0,'TRUNCATE')
				map_param[param] = actions
	tables = map_param.pop("TABLES").replace(' ','').split(',')

	list_maps = []
	for table in set(tables):
		tmp_map = map_param.copy()

		tmp_map["TABLE"] = table;
		list_maps.append(tmp_map)
	return list_maps


def managePartition(map_param, str_tasktime, action):
	#print getCurTime(), "map_param:",map_param
	table_type = map_param.get("TYPE")
	tasktime = datetime.strptime(str_tasktime, '%Y%m%d')
	str_endtime = (tasktime + timedelta(days = 1)).strftime('%Y%m%d')
	str_deltime = (tasktime - timedelta(days = map_param['KEEP_DAY'])).strftime('%Y%m%d')
	#print getCurTime(), str_deltime

	sql = ""
	all_partitions = getAllPartitions(map_param)
	if all_partitions is None:
		return -1
	if table_type == 'DAY':
		if action == 'ADD':
			if str_tasktime not in all_partitions:
				sql = SQL_ADD % (map_param.get('TABLE'), "PARTITION p_%s VALUES LESS THAN (%s)" % (str_tasktime, str_endtime))
			else:
				print getCurTime(), "skip add table",map_param['TABLE'],'partition',str_tasktime,', already have.'
				return 0
		elif action == 'TRUNCATE':
			if str_deltime in all_partitions:
				sql = SQL_TRUNCATE % (map_param.get('TABLE'), 'p_' + str_deltime)
			else:
				print getCurTime(), "skip truncate, table",map_param['TABLE'],'partition',str_deltime,' not exits.'
				return 0
		elif action == 'DROP':
			if str_deltime in all_partitions:
				sql = SQL_DROP % (map_param.get('TABLE'), 'p_' + str_deltime)
			else:
				print getCurTime(), "skip drop, table",map_param['TABLE'],'partition',str_deltime,' not exits.'
				return 0
	elif table_type == 'HOUR':
		if action == 'ADD':
			if str_tasktime+'00' not in all_partitions:
				sql = SQL_ADD % (map_param.get('TABLE'), ', '.join([ '\n'*(1 if x%3==0 else 0)+ ' '*(10 if x%3==0 else 0) + "PARTITION p_%s VALUES LESS THAN (%s)" \
					% (str_tasktime + str(x).zfill(2), str_tasktime + str(x+1).zfill(2)) for x in range(0,23) ]) \
					+ (", PARTITION p_%s23 VALUES LESS THAN (%s00)" % (str_tasktime,str_endtime)))
			else:
				print getCurTime(), "skip add table",map_param['TABLE'],'partition',str_tasktime,'[00~23], already have.'
				return 0
		elif action == 'TRUNCATE':
			if str_deltime+'00' in all_partitions:
				sql = SQL_TRUNCATE % (map_param.get('TABLE'), ','.join([ '\n'*(1 if x%6==0 else 0)+ ' '*(10 if x%6==0 else 1) + 'p_' + str_deltime + str(x).zfill(2) for x in range(0,24)]))	
			else:
				print getCurTime(), "skip truncate, table",map_param['TABLE'],'partition',str_deltime,'[00~23] not exits.'
				return 0
		elif action == 'DROP':
			if str_deltime+'00' in all_partitions:
				sql = SQL_DROP % (map_param.get('TABLE'), ','.join([ '\n'*(1 if x%6==0 else 0)+ ' '*(10 if x%6==0 else 1) + 'p_' + str_deltime + str(x).zfill(2) for x in range(0,24)]))
			else:
				print getCurTime(), "skip drop, table",map_param['TABLE'],'partition',str_deltime,'[00~23] not exits.'
				return 0
	if sql is not None or sql != '':
		print getCurTime(), '[EXECUTE]',sql
		retcode,msg = excuteSql(map_param,sql)
		print getCurTime(), '[retcode:',retcode,'\tmsg=>',msg,']'

def main(argv=None):
	taskTime = datetime.now().strftime('%Y%m%d') 
	if(len(argv) == 2):
		taskTime = argv[1]
	elif len(argv) > 2 :
		print getCurTime(), 'err param'
		return -1
	print getCurTime(), 'taskTime is',taskTime

	conf = ConfigParser.ConfigParser()
	conf.read('./table.conf')
	for each_section in conf.sections():
		list_maps = checkAndGetSectionMap(conf,each_section)
		if list_maps is None:
			print getCurTime(), "[ERROR]Error when parse section:",each_section
			continue
		print getCurTime(), '-'*20,'start section:',each_section,'TYPE:',list_maps[0]['TYPE'],'-'*20
		for map_param in list_maps:
			#print getCurTime(), map_param
			list_actions = map_param.pop("ACTION")
			for action in list_actions:
				managePartition(map_param,taskTime,action)
				
			
			


if __name__ == "__main__":
	sys.exit(main(sys.argv))