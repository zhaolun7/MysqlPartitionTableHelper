#-*- coding: utf-8 -*-
# author:zhaolun
# the script help managing partitioned table by reading `table.conf`, it should be run with crond.
import os,sys,commands,re,ConfigParser,time,threading,logging
from datetime import datetime, timedelta
SERVER_CONNENT = "mysql -h%(HOSTIP)s -P%(PORT)s -u%(USER)s -p%(PASSWORD)s -D%(DATABASE)s --default-character-set=utf8 -A -N -e '%(SQL)s' "
SQL_TRUNCATE   = "alter table %s truncate partition %s"
SQL_DROP       = "alter table %s drop partition %s"
SQL_ADD        = "alter table %s add partition (%s)"
list_param = ["HOSTIP","PORT","USER","PASSWORD","DATABASE","TABLES","KEEP_DAY","TYPE","ACTION"]

THREAD_LOCK = threading.RLock()
#DEBUG_MODE = False
def getCurTime():
    return datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')

def excuteSql(sectionMap,excute_sql):
    sectionMap = sectionMap.copy()
    sectionMap["SQL"] = excute_sql
    command_execute = SERVER_CONNENT % sectionMap
    
    status,output = commands.getstatusoutput(command_execute)
    #if status != 0:
    #    print '[ERROR]',output
    return status,output.replace('Warning: Using a password on the command line interface can be insecure.','')
def getAllPartitions(map_param, logger):

    status, output = excuteSql(map_param,"show create table " + map_param['TABLE'])
    if status != 0:
        logger.error("get partiton faild: msg=> %s",output)
        return None
    
    m = re.compile(r'PARTITION p_(\d+) VALUES LESS THAN \(\d+\)')
    partitions = []
    for line in output.split(r'\n'):
        r = m.search(line)
        if r is not None:
            partitions.append(r.group(1))
    return partitions


def checkAndGetSectionMap(configParser,section,logger):
    map_param = {}
    for param in list_param:
        tmp_str = configParser.get(section, param)
        if tmp_str == '' or tmp_str is None:
            logger.error("section:%s,param:%s,values is %s", section,param,tmp_str)
            return None
        else:
            map_param[param] = tmp_str
            if param == "TYPE":
                tmp_str = tmp_str.upper()
                #print getCurTime(), tmp_str
                if tmp_str not in ["DAY","HOUR"]:
                    logger.error("%s TYPE ERROR:%s", section,tmp_str)
                    return None
                map_param[param] = tmp_str
            elif param  == 'KEEP_DAY':
                if tmp_str.isdigit():
                    map_param[param] = int(tmp_str)
                else:
                    logger.error("%s KEEP_DAY ERROR:%s", section,tmp_str)
                    return None
            elif param == "ACTION":
                real_actions = []
                actions = tmp_str.upper().split(',')
                for action in actions:
                    action = action.strip()
                    if action not in ['ADD','DROP','TRUNCATE']:
                        logger.error("%s ACTION ERROR:%s", section,tmp_str)
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


def managePartition(query_id, map_param, str_tasktime, action, logger):
    table_type = map_param.get("TYPE")
    tasktime,str_endtime,str_deltime = None,None,None
    with THREAD_LOCK:
        tasktime = datetime.strptime(str_tasktime, '%Y%m%d')
        str_endtime = (tasktime + timedelta(days = 1)).strftime('%Y%m%d')
        str_deltime = (tasktime - timedelta(days = map_param['KEEP_DAY'])).strftime('%Y%m%d')

    sql = None
    sql_print = None
    all_partitions = getAllPartitions(map_param,logger)
    if all_partitions is None:
        return None,None
    if table_type == 'DAY':
        if action == 'ADD':
            if str_tasktime not in all_partitions:
                sql = SQL_ADD % (map_param.get('TABLE'), "PARTITION p_%s VALUES LESS THAN (%s)" % (str_tasktime, str_endtime))
            else:
                logger.info("[SKIP %s](%s) skip add table %s partition %s, already have.", action, query_id, map_param['TABLE'],str_tasktime)
                return None,None
        elif action == 'TRUNCATE':
            if str_deltime in all_partitions:
                sql = SQL_TRUNCATE % (map_param.get('TABLE'), 'p_' + str_deltime)
            else:
                logger.info('[SKIP %s](%s) skip truncate table %s partition %s not exits.', action, query_id, map_param['TABLE'], str_deltime)
                return None,None
        elif action == 'DROP':
            if str_deltime in all_partitions:
                sql = SQL_DROP % (map_param.get('TABLE'), 'p_' + str_deltime)
            else:
                logger.info('[SKIP %s](%s) skip drop table %s partition %s not exits.', action, query_id, map_param['TABLE'], str_deltime)
                return None,None
    elif table_type == 'HOUR':
        if action == 'ADD':
            if str_tasktime+'00' not in all_partitions:
                sql = SQL_ADD % (map_param.get('TABLE'), ', '.join([ '\n'*(1 if x%3==0 else 0)+ ' '*(10 if x%3==0 else 0) + "PARTITION p_%s VALUES LESS THAN (%s)" \
                    % (str_tasktime + str(x).zfill(2), str_tasktime + str(x+1).zfill(2)) for x in range(0,23) ]) \
                    + (", PARTITION p_%s23 VALUES LESS THAN (%s00)" % (str_tasktime,str_endtime)))
                sql_print = SQL_ADD % (map_param.get('TABLE'), 'PARTITION p_%s[00~23] VALUES LESS THAN (%s[01~23]|%s00)' % (str_tasktime,str_tasktime,str_endtime))
            else:
                logger.info('[SKIP %s](%s) skip add table %s partition %s[00~23], already have.', action, query_id, map_param['TABLE'], str_tasktime)
                return None,None
        elif action == 'TRUNCATE':
            if str_deltime+'00' in all_partitions:
                sql = SQL_TRUNCATE % (map_param.get('TABLE'), ','.join([ '\n'*(1 if x%6==0 else 0)+ ' '*(10 if x%6==0 else 1) + 'p_' + str_deltime + str(x).zfill(2) for x in range(0,24)]))
                sql_print = SQL_TRUNCATE % (map_param.get('TABLE'), str_deltime+'[00~23]')
            else:
                logger.info('[SKIP %s](%s) skip truncate table %s partition %s[00~23] not exits.', action, query_id, map_param['TABLE'],str_deltime)
                return None,None
        elif action == 'DROP':
            if str_deltime+'00' in all_partitions:
                sql = SQL_DROP % (map_param.get('TABLE'), ','.join([ '\n'*(1 if x%6==0 else 0)+ ' '*(10 if x%6==0 else 1) + 'p_' + str_deltime + str(x).zfill(2) for x in range(0,24)]))
                sql_print = SQL_DROP % (map_param.get('TABLE'), str_deltime+'[00~23]')
            else:
                logger.info('[SKIP %s](%s) skip drop table %s partition %s[00~23] not exits.', action, query_id, map_param['TABLE'],str_deltime)
                return None,None
    return sql,sql_print


class SingleTableActionThread (threading.Thread):
    def __init__(self, query_id, map_param, taskTime, logger):
        threading.Thread.__init__(self)
        self.query_id = query_id
        self.map_param = map_param
        self.taskTime = taskTime
        self.logger = logger
        self.sql = None 
        self.sql_print = None
    def run(self):
        list_actions = self.map_param.pop("ACTION")
        starttime = time.time()
        for action in list_actions:
            sql,sql_print = managePartition(self.query_id,self.map_param,self.taskTime,action,self.logger)
            self.sql = sql
            self.sql_print = sql_print
            query_id,logger = self.query_id,self.logger
            if sql is not None and sql != '':
                if sql_print != None:
                    logger.info('[EXEC %s](%s) %s', action, query_id, sql_print)
                else:
                    logger.info('[EXEC %s](%s) %s', action, query_id, sql)
                retcode,msg = excuteSql(self.map_param,sql)
                endtime = time.time()
                usetime = endtime - starttime
                if retcode == 0:
                    logger.info('[SUCC %s](%s) [RETCODE:%d, msg=> %s][usedTime:%ss]', action, query_id, retcode, msg,usetime)
                else:
                    logger.error('[FAIL %s](%s) [RETCODE:%d, msg=> %s][usedTime:%ss],sql is: %s', action, query_id, retcode, msg,usetime, self.sql)
        

def main(argv=None):
    logging.basicConfig(level=logging.INFO, 
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.join(os.path.split(os.path.realpath(__file__))[0],'crond.log'),
                        format='%(asctime)s [%(levelname)s] %(message)s')
    logger = logging.getLogger(__name__)
    taskTime = datetime.now().strftime('%Y%m%d') 
    if(len(argv) == 2):
        taskTime = argv[1]
    elif len(argv) > 2 :
        logger.info('err param')
        return -1
    logger.info('taskTime is %s',taskTime)
    
    conf = ConfigParser.ConfigParser()
    conf.read(os.path.join(os.path.split(os.path.realpath(__file__))[0],'table.conf'))
    for each_section in conf.sections():
        list_maps = checkAndGetSectionMap(conf,each_section,logger)
        if list_maps is None:
            logger.error('Error when parse section: %s', each_section)
            continue
        
        tableCnt = 0
        for map_param in list_maps:
            t = SingleTableActionThread(each_section + "-%03d" % tableCnt, map_param, taskTime, logger)
            while threading.activeCount() > 10:
                time.sleep(0.1)
            t.start()
            tableCnt += 1
    while threading.activeCount() != 1:
        time.sleep(10)
        if threading.activeCount() != 1:
            list_t = threading.enumerate()
            for t in list_t:
                if t.getName() != 'MainThread':
                    if t.sql_print is not None:
                        logger.info('[WAIT SQL] wait query:%s', t.sql_print)
                    else:
                        logger.info('[WAIT SQL] wait query:%s', t.sql)
    logger.info('script exit.')
if __name__ == "__main__":
    sys.exit(main(sys.argv))
