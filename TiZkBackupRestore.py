# -*- coding: utf-8 -*-
"""
@version: 1.0
@author: 姜勇平
@email: idealage@126.com
@license: Apache Licence
@file: TiZkBackupRestore.py
@time: 2020-07-08
@remark: 
  1. pip install Kazoo
  2. 备份与恢复
"""

import json, argparse
from kazoo import security
from kazoo.client import KazooClient
from xml.dom.minidom import Document
from xml.etree import ElementTree


# 基类
class TiZkBackupRestoreBase(object):
    ZK_ROOT = 'zk_root'     # zk根"/"的标志定义

    def __init__(self, hosts, auth_data=None, print_debug=True):
        """
        构造
        :param hosts: 主机信息
        :param auth_data: 授权信息，如:[('digest', 'jyp:123456')]
        :param print_debug: 是否打印调试信息
        """
        self.level = 0
        self.node_count = 0
        self.print_debug = print_debug
        self.zk_connect = None

        try:
            self.zk_connect = KazooClient(hosts=hosts, auth_data=auth_data)
            self.zk_connect.start()
        except Exception as e:
            print('zk connect error: {}'.format(e))

    def __del__(self):
        self.zk_connect.stop()


    @staticmethod
    def split_acls(acl_str):
        """
        解析ACL数据
        :param acl_str: 支持多个权限，以逗号分隔，如："digest=jyp:123456,digest=jyp2:123456"
        :return:
        """
        acls = list()
        for acl in acl_str.split(','):
            acls.append(tuple(acl.split('=')))

        return acls


# 备份类
class TiZkBackup(TiZkBackupRestoreBase):
    def __init__(self, filename, **kwargs):
        """
        构造
        :param filename: 要保存的文件名
        """
        super(TiZkBackup, self).__init__(**kwargs)
        self.filename = filename

        self.xml_obj = Document()
        self.xml_root = self.xml_obj.createElement("ROOT")
        self.xml_obj.appendChild(self.xml_root)


    def backup(self, path):
        """
        开始处理
        :param path:   要处理的路径
        """
        # 递归读取节点
        self.get_node(path, self.xml_root)

        # 保存文件
        with open(self.filename, 'w') as f:
            self.xml_obj.writexml(f, indent='', addindent='  ', newl='\n', encoding='utf-8')

        print(f'备份完成，共备份节点:{self.node_count}，backup file:{self.filename}\n')

    def get_node(self, path, xml_parent):
        """
        读取节点
        :param path: 路径
        :param xml_parent: xml父节点
        """
        self.level += 1

        # 生成当前xml节点
        xml_name = path.split('/')[-1]
        xml_name = self.ZK_ROOT if xml_name == '' else xml_name
        xml_node = self.xml_obj.createElement(xml_name)

        try:
            # 获取节点列表
            child_nodes = self.zk_connect.get_children(path)
            if child_nodes:
                # 处理当前节点
                self.save_node(path, True, xml_node)

                # 处理子节点
                for child in child_nodes:
                    link_char = '' if path == '/' else '/'
                    self.get_node(path + link_char + child, xml_node)
            else:
                self.save_node(path, False, xml_node)
        except Exception as e:
            print('get_node() error: {}'.format(e))

        xml_parent.appendChild(xml_node)
        self.level -= 1


    def save_node(self, path, is_parent, xml_node):
        """
        保存节点
        :param path: 路径
        :param is_parent: 是否是父节点
        :param xml_node: xml节点
        :return:
        """
        try:
            self.node_count += 1
            data, stat = self.zk_connect.get(path)
            acls = self.zk_connect.get_acls(path)

            xml_node.setAttribute('value', data.decode('utf8'))
            xml_node.setAttribute('ephemeral', str(stat.ephemeralOwner))

            # 生成acl的json格式数据，为了美观替换了"为'，所以在读取时也要反向操作
            acl_str = json.dumps({"perms": acls[0][0].perms, "scheme": acls[0][0].id.scheme, "id": acls[0][0].id.id})
            xml_node.setAttribute('acl', acl_str.replace('"', "'"))

            if self.print_debug:
                if is_parent:
                    print(f'L{self.level}\t' + ' ' * self.level + f'+{path}, data={data}, acl={acls[0]}')
                else:
                    print(f'L{self.level}\t' + ' ' * self.level + f' {path}, data={data}, acl={acls[0]}')
        except Exception as e:
            print('save_node() error: {}'.format(e))


# 恢复类
class TiZkRestore(TiZkBackupRestoreBase):
    def __init__(self, filename, **kwargs):
        """
        构造
        :param filename: 要读取的文件名
        """
        super(TiZkRestore, self).__init__(**kwargs)
        self.filename = filename


    def restore(self, path):
        """
        开始处理
        :param path:   要处理的根路径
        """
        try:
            xml_obj = ElementTree.parse(self.filename)
            xml_root = xml_obj.getroot()[0]
            new_path = '' if xml_root.tag == self.ZK_ROOT else path
            self.put_node(new_path, xml_root)
        except Exception as e:
            print('restore error: {}'.format(e))
            return False

        print(f'恢复完成，共恢复节点:{self.node_count}，restore file:{self.filename}\n')
        return True


    def put_node(self, path, xml_node):
        """
        读取节点
        :param path: 路径
        :param xml_node: xml点
        """
        self.level += 1

        try:
            # 获取节点列表
            child_nodes = xml_node.getchildren()
            if child_nodes:
                # 处理当前节点
                self.load_node(path, True, xml_node)

                # 处理子节点
                for child in child_nodes:
                    link_char = '' if path == '/' else '/'
                    self.put_node(path + link_char + child.tag, child)
            else:
                self.load_node(path, False, xml_node)
        except Exception as e:
            print('put_node() error: {}'.format(e))

        self.level -= 1


    def load_node(self, path, is_parent, xml_node):
        """
        保存节点
        :param path: 路径
        :param is_parent: 是否是父节点
        :param xml_node: xml节点
        :return:
        """
        try:
            # 读取节点分析
            value = xml_node.attrib['value']
            ephemeral = xml_node.attrib['ephemeral']
            acl_str = xml_node.attrib['acl'].replace("'", '"')
            acl_data = json.loads(acl_str)

            # 创建或更新节点
            self.node_count += 1
            acl_rule = security.ACL(acl_data['perms'], security.Id(acl_data['scheme'], acl_data['id']))
            if self.zk_connect.exists(path):
                self.zk_connect.set(path, value.encode('utf8'))
                self.zk_connect.set_acls(path, [acl_rule])
            else:
                ephemeral = True if ephemeral == '1' else False
                self.zk_connect.create(path, value.encode('utf8'), [acl_rule], ephemeral=ephemeral)

            # 打印调试信息
            if self.print_debug:
                if is_parent:
                    print(f'L{self.level}' + ' ' * self.level + f'+{path}, data={value}, acl={acl_str}')
                else:
                    print(f'L{self.level}' + ' ' * self.level + f' {path}, data={value}, acl={acl_str}')
        except Exception as e:
            print('load_node() error: {}'.format(e))


# 测试
if __name__ == '__main__':
    is_test = False

    # 测试模式
    if is_test:
        zk_serv_src = '10.0.10.112:2181'
        zk_serv_dst = '10.0.10.112:2181'
        zk_auth = [('digest', 'jyp:123456')]
        zk_file = './zk_backup.xml'
        zk_root = '/'
        zkb = TiZkBackup(zk_file, hosts=zk_serv_src, auth_data=zk_auth, print_debug=True)
        zkb.backup(zk_root)
        del zkb

        zkr = TiZkRestore(zk_file, hosts=zk_serv_dst, auth_data=zk_auth, print_debug=True)
        zkr.restore(zk_root)
        del zkr
    else:
        # 解析参数
        parser = argparse.ArgumentParser(description='zookeeper backup an restore.')
        parser.add_argument('-t', '--type', type=str, default='backup', help='backup or restore.')
        parser.add_argument('-p', '--path', type=str, default='/', help='zookeeper path.')
        parser.add_argument('-f', '--file', type=str, default='zk_backup.xml', help='backup or restore file.')
        parser.add_argument('-s', '--hosts', type=str, default='127.0.0.1:2181', help='zookeeper server:port.')
        parser.add_argument('-a', '--auth', type=str, default=None, help='permission, support multiple permissions，example："digest=jyp:123456,digest=jyp2:123456"')
        parser.add_argument('-d', '--debug', help='print debug info.', action="store_true")
        cmd_params = parser.parse_args()

        auth_data = None if cmd_params.auth is None else TiZkBackupRestoreBase.split_acls(cmd_params.auth)
        if cmd_params.type == 'backup':
            zkb = TiZkBackup(cmd_params.file, hosts=cmd_params.hosts, auth_data=auth_data, print_debug=cmd_params.debug)
            zkb.backup(cmd_params.path)
            del zkb
        else:
            zkr = TiZkRestore(cmd_params.file, hosts=cmd_params.hosts, auth_data=auth_data, print_debug=cmd_params.debug)
            zkr.restore(cmd_params.path)
            del zkr
