import pandas as pd
import os
import numpy as np

def load_meta_data(file_path):
    """
    加载元数据文件
    """
    _, file_extension = os.path.splitext(file_path)

    if file_extension == '.xls':
        metadata = pd.read_excel(file_path)
    else:
        raise "格式不支持"
    return metadata


def scan_data(datapath):
    """
    扫描数据目录，返回子目录列表和模态集合
    """
    sub_list = os.listdir(datapath)
    modal_set = set()
    for sub in sub_list:
        modal_list = os.listdir(os.path.join(datapath, sub))
        for modal in modal_list: 
            if '_' in modal and modal.split('_')[-1].isdigit():
                modal_set.add('_'.join(modal.split('_')[:-1]))
            else:
                modal_set.add(modal)#确保只删除模态文件的扫描后缀
    return sub_list, modal_set


def return_metadata(metadata, subid_name, subid):
    """
    返回指定subid相应元数据
    """
    # 这里假设元数据中有一个名为'SUB_ID'的列
    if metadata[metadata[subid_name]==subid].empty:
        data = {
            'IXI_ID': subid,
        }
        is_empty = True
    else:
        data = metadata[metadata[subid_name]==subid].to_dict(orient='list')
        delet_keys = metadata[metadata[subid_name]==subid].isnull().to_dict(orient='list')
        for k in data:
            data[k] = data[k][0]
        for k in delet_keys:
            if delet_keys[k][0]:
                del data[k]
        is_empty = False
    return data, is_empty