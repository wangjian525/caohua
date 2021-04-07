from ip2Region import Ip2Region  # 此处导入的是刚才复制的py文件
db_file = './ip2region.db'  # 数据库文件路径
search_file = './search_file.txt'  # 查询文件：每行一个ip
result_file = './result_file.txt'  # 结果文件：每行一个ip结果


def ip_search():

    # 打开需要查询的文件
    with open(search_file, 'r', encoding='utf-8') as search_file_r:
        search_file_lines = search_file_r.readlines()

    # 打开结果文件，准备写入
    with open(result_file, 'a', encoding='utf-8') as result_file_w:
        searcher = Ip2Region(db_file)  # 实例化
        for line in search_file_lines:
            ip = line.strip('\n')
            # 判断是不是ip，isip 这个函数是Ip2Region里写好的，直接用
            if searcher.isip(ip):
                # 三种算法任选其一
                data = searcher.btreeSearch(ip)  # B树
                # data = searcher.binarySearch(line) # 二进制
                # data = searcher.memorySearch(line) # 内存
                result_file_w.write("%s|%s\n" % (ip, data["region"].decode('utf-8')))
                print("%s|%s" % (ip, data["region"].decode('utf-8')))
            else:
                result_file_w.write('%s|错误数据\n' % ip)
                print('%s|错误数据' % ip)
    searcher.close()  # 关闭


if __name__ == "__main__":

    ip_search()


