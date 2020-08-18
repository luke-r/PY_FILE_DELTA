import hashlib as hl
import datetime as dt
import numpy as np
import pandas as pd

class delta:   
    def __init__(self, obj = None):
        self.obj = obj
        return None
    
    #dictionary to verify hash algorithm is valid
    _algdict = {'sha256':hl.sha256
               ,'md5':hl.md5
               ,'sha1':hl.sha1
               ,'sha224':hl.sha224
               ,'sha384':hl.sha384
               ,'sha512':hl.sha512}
    
    #sets hash algorithm for class
    def _return_hash(self,obj=None,alg = 'sha256',):
        try:
            function=self._algdict[alg]
        except KeyError:
            raise ValueError('Bad hash algorithm name passed.')
        if obj == None:
            return self._algdict[alg]
        else:
            try:
                obj.decode('utf-8')
            except AttributeError:
                obj = obj.encode('utf-8')
                #use below if want to force passing an encoding string instead of converting auto
                #print("string is not UTF-8")
                #raise
            x = self._algdict[alg]
            return x(obj)    
       
    #identifies delta
    def result(self,oldfile, newfile, exportfile, block = 65536, statistics = False, alg = 'sha256'):
        if statistics == True:
            row_assess_begin = dt.datetime.now()
        
        hash_old_file_row = []
        with open(oldfile) as file:
            for line in file:
                line = line.strip().encode('utf-8') 
                hasher_old_row = self._return_hash(line)
                hash_old_file_row.append(hasher_old_row.hexdigest())
        
        df1 = pd.read_csv(oldfile,sep=',',usecols = ['ID'],dtype = {'ID':'Int32'})
        
        hash_new_file_row = []
        with open(newfile) as file:
            for line in file: 
                line = line.strip().encode('utf-8') 
                hasher_new_row = self._return_hash(line)
                hash_new_file_row.append(hasher_new_row.hexdigest())

        hash1_np = np.asarray(hash_old_file_row)
        hash2_np = np.asarray(hash_new_file_row)
        x1 = np.setdiff1d(hash1_np,hash2_np,assume_unique=False).tolist()
        hash_index = []
        for n, i in enumerate(x1):
            hash_index.append(df1.loc[hash_old_file_row.index(x1[n])] - 1)  
        
        if statistics == True:
            row_assess_end = dt.datetime.now()
            row_assess_elapse = row_assess_end - row_assess_begin
            row_retrieval_start = dt.datetime.now()
        
        df2 = pd.read_csv(newfile,sep=',',nrows=0,dtype={'ID': np.int64})
        for chunk in pd.read_csv(newfile,sep=',',chunksize=block):
            df2 = df2.append(chunk[chunk['ID'].isin(hash_index)])
        
        if statistics == True:      
            row_retrieval_end = dt.datetime.now()
            row_retrieval_elapse = row_retrieval_end - row_retrieval_start
            row_export_start = dt.datetime.now()
        
        #exporting file if needed
        if exportfile == False:
            if statistics == True:
                row_export_end = dt.datetime.now()
                row_export_elapse = row_export_end - row_export_start
        else:
            df2.to_csv(export_file)
            if statistics == True:
                row_export_end = dt.datetime.now()
                row_export_elapse = row_export_end - row_export_start
                
        #preparing results
        if exportfile == False:
            if statistics == True:
                compiled_statistics = {
                    'row_assess_begin':row_assess_begin,
                    'row_assess_end':row_assess_end,
                    'row_assess_elapse':row_assess_elapse,
                    'row_retrieval_start':row_retrieval_start,
                    'row_retrieval_end':row_retrieval_end,
                    'row_retrieval_elapse':row_retrieval_elapse,
                    'row_export_start':row_export_start,
                    'row_export_end':row_export_end,
                    'row_export_elapse':row_export_elapse
                }
                return df2,compiled_statistics
            return df2
        else:
            if statistics == True:
                compiled_statistics = {
                    'row_assess_begin':row_assess_begin,
                    'row_assess_end':row_assess_end,
                    'row_assess_elapse':row_assess_elapse,
                    'row_retrieval_start':row_retrieval_start,
                    'row_retrieval_end':row_retrieval_end,
                    'row_retrieval_elapse':row_retrieval_elapse,
                    'row_export_start':row_export_start,
                    'row_export_end':row_export_end,
                }
                return compiled_statistics
            return None
