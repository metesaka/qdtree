import os
def trans(in_file):
    with open('query_sql/' + in_file) as f:
        data = f.read()
    
    data_sep = data.split('\n\n')
    out_name = in_file.replace('qf','').split('.')[0]
    for i in range(1,len(data_sep)):
        with open('queries2/'+str(i)+'.'+out_name+'.sql','w') as o:
            o.write(data_sep[i])
    
def main():
    sqls = os.listdir('query_sql')
    sqls = [i for i in sqls if i.endswith('.sql')]
    for i in sqls:
        trans(i)

main()