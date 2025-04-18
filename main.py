from utils.database_utils import *
from utils.tool_utils import *
from tqdm import *

def main(root, passwd, database_name, datapath, metapath):
    """
    主函数
    """
    # 初始化数据库
    db = Database(root, passwd, database_name)

    #加载元数据
    metadata = load_meta_data(metapath)

    #扫描文件查看数据包括几种模态
    sub_list, modal_set = scan_data(datapath)

    #创建数据库样本表(这里需要根据metadata内容来构建)
    '''
    可选项: 
    INT, VARCHAR(*), FLOAT, DATE, TEXT
    PRIMARY KEY, NOT NULL
    '''
    subject_keys = {'IXI_ID': 'INT PRIMARY KEY', 
                    'SEX': 'CHAR(1)', 
                    'HEIGHT': 'FLOAT', 
                    'WEIGHT': 'FLOAT', 
                    'ETHNIC_ID': 'INT',
                    'MARITAL_ID': 'INT',
                    'OCCUPATION_ID': 'INT', 
                    'QUALIFICATION_ID': 'INT', 
                    'DOB': 'DATE',
                    'DATE_AVAILABLE': 'INT', 
                    'STUDY_DATE': 'DATETIME', 
                    'AGE': 'FLOAT'}
    
    columns = ', '.join([f"{key} {datatype}" for key, datatype in subject_keys.items()])
    db.create_table('subject', columns=columns)


    #创建各模态数据表
    modals_keys = {
        'DATA_PATH': 'VARCHAR(255)',
        'PROCESSED_PATH': 'VARCHAR(255)',
        'SCAN_DATE': 'DATE',
        'SCAN_ID': 'INT',
        'META_PATH': 'VARCHAR(255)',
        'SUB_ID': 'INT NOT NULL',
    }
    #设置外键（需要修改）
    foreign_keys = 'FOREIGN KEY (SUB_ID) REFERENCES subject(IXI_ID) ON DELETE CASCADE'
    
    modal_columns = ', '.join([f"{key} {datatype}" 
                               for key, datatype in modals_keys.items()] + [foreign_keys])
    for modal in modal_set:
        db.create_table(modal, columns=modal_columns)

    
    #扫描数据目录，插入数据
    for sub in tqdm(sub_list):
        sub_path = os.path.join(datapath, sub)
        #插入subject表(同样根据具体情况修改)
        sub_data, is_empty = return_metadata(metadata, 'IXI_ID', int(sub[3:]))

        
        if is_empty==False:
            #微调一下数据（需要手动修改，没有修改可以pass）
            if 'SEX_ID (1=m, 2=f)' in sub_data:
                sub_data['SEX'] = 'm' if sub_data['SEX_ID (1=m, 2=f)'] == 1 else 'f'
                del sub_data['SEX_ID (1=m, 2=f)']
            #####

        #assert sub_data.keys() == subject_keys.keys(), 'subject表属性不匹配'
        db.insert_data('subject', sub_data)

        #插入模态表
        for modal in os.listdir(sub_path):
            modal_path = os.path.join(sub_path, modal)
            scan_id = 1 #默认扫描序列为1
            if '_' in modal and modal.split('_')[-1].isdigit():
                modal_name = '_'.join(modal.split('_')[:-1])
                scan_id = int(modal.split('_')[-1])#更新扫描序列
            else:
                modal_name = modal
            # 插入数据(根据模态表插入，data中没有的就是NULL)
            data = {
                'DATA_PATH': modal_path,
                'SCAN_ID': scan_id,
                'SUB_ID': int(sub[3:]),#根据sub文件名修改
            }
            db.insert_data(modal_name, data)
    
    db.free()
    print('入库完成')



if __name__ == '__main__':
    root = 'root'
    passwd = 'liu123yan456'
    database_name = 'IXI'

    #数据根目录
    datapath = '/home/DATABASE/IXI_tianmingxiao/collect'

    #样本元路径，主要是年龄性别这些
    metapath = '/home/DATABASE/IXI_tianmingxiao/IXI.xls'

    #入库主函数
    main(root=root,
         passwd=passwd,
         database_name=database_name,
         datapath=datapath, 
         metapath=metapath)

