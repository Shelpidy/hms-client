import os
import json
import uuid
from dataclasses import dataclass,field
import datetime
from functools import wraps

class Collection:
    columns = []
    def __init__(self,document_name,columns,collection_folder = None) -> None:
        if not(isinstance(document_name,str)):
             raise ValueError("The collection name must be a string")
        if not(isinstance(columns,list)):
             raise ValueError("columns must of be of type list")
        if (len(columns) < 1):
             raise Exception("columns must not be an empty list")
        self.filename = document_name+".db" if collection_folder == None else ("./" + collection_folder + "/" + document_name+".db")
        if not(os.path.exists('./'+collection_folder)):
            os.mkdir(collection_folder)
        self._id = 1
        self.columns = columns
        self.all_data =  self.fetch_all()

    def __len__(self):
        return len(self.fetch_all())

    def __str__(self) -> str:
        return json.dumps(self.fetch_all())

    def view(self):
        _data = self.all_data
        values_len = max(list(map(lambda x:len(str(list(x.values())[0])),_data)))
        keys_len = max(list(map(lambda x:len(str(list(x.keys())[0])),_data)))
        _len = max([values_len,keys_len])

        print(f"\n\n|||||||||||||||||||||||||||||||||||||||||| Document Path => {self.filename} |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n")
        print(f"{''*(_len - len('No |'))}    No | ",end ="")
        for col in self.columns:
                print(f"{' '*(_len - len(str(col)))}{col}  |",end='')
        print("\n")
        print("    ",end='')
        for column in range(len(self.columns) + 2):
            print(f"{'='*_len}",end='')
        print("\n")
        for column in _data:
             print(f"{' '*(_len - len('_id |'))}{column.get('_id')}  |",end='')
             for col in self.columns:
                print(f"{' '*(_len - len(str(column.get(col))))}{column.get(col)}  |",end='')
             print("\n")
        print("\n\n|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n")

    def validate_columns(self,data):
        uid = str(uuid.uuid1())
        _data = data
        keys = data.keys()
        for key in keys:
            if key not in self.columns:
                raise Exception(f"The column, {key}, doesn't exist.")
        for column in self.columns:
            if column not in keys:
                _data[column] = None
        _data['id'] = uid
        return _data

    def add_record(self,data = {}):
        _data = self.validate_columns(data)
        with open(self.filename,"a") as file:
            record =json.dumps(_data)+"\n"
            file.write(record)
            return self

    def get_with_id(self,data):
        if data != "\n":
            data = json.loads(data.strip())
            data["_id"] = self._id
            self._id += 1
            return data

    def fetch_all(self,view = False):
        if not(os.path.exists(self.filename)):
            with open(self.filename,'w') as f:
                return  
        self._id = 1

        with open(self.filename,"r") as file:
            valid_data = list(filter(lambda x: x != "\n",file.readlines()))
            self.all_data = list(map(self.get_with_id,valid_data))
            if view:
                self.view()
            return self.all_data

    def fetch_many(self,data,view = False):
        try:
            _data = self.fetch_all()
            self.all_data = list(filter(lambda x:x == data or set(data.values()).issubset(set(x.values())),_data))
            if view:
                self.view()
            return self.all_data
        except:
            return []

    def fetch_one(self,data,view = False):
        try:
            self.all_data = [self.fetch_many(data)[0]]
            if view:
                self.view()
            return self.all_data[0]
        except:
            return {}

    def drop_collection(self,remove_path = False)->None:   
        if (os.path.exists(self.filename)):
            if remove_path:
                os.remove(self.filename)
                os.rmdir(self.filename.rsplit("/",1)[0])
                return 
            os.remove(self.filename)


    def reset_collection(self) ->None:
        with open(self.filename,"r+") as f:
            f.truncate()

    def delete_record(self,constraint,view = False):
        with open(self.filename,"r+") as f:
            final_list_to_be_written = []
            lines = list(filter(lambda x: x != "\n",f.readlines()))
            f.seek(0)
            f.truncate()
            _lines = list(enumerate(lines))
            ns = list(filter(lambda x:json.loads(x[1].strip()) == constraint or set(constraint.values()).issubset(set(json.loads(x[1].strip()).values())),_lines))
            if len(ns) < 1:
                raise Exception("Constraint doesn't match any data in the database")
            _ns = list(map(lambda x: x[0],ns))
            for line,d in _lines:
                if line in _ns:
                    __d = json.loads(d)
                    print(__d)
                    continue
                else:
                    final_list_to_be_written.append(d)
            f.writelines(final_list_to_be_written)
            valid_data = list(filter(lambda x: x != "\n",final_list_to_be_written))
            self.all_data = list(map(self.get_with_id,valid_data))
            print(self.all_data)
            if view:
                self.view()

    def update_record(self,data,constraint,view = False):

        with open(self.filename,"r+") as f:
            final_list_to_be_written = []
            lines = list(filter(lambda x: x != "\n",f.readlines()))
            f.seek(0)
            f.truncate()
            _lines = list(enumerate(lines))
            ns = list(filter(lambda x:json.loads(x[1].strip()) == constraint or set(constraint.values()).issubset(set(json.loads(x[1].strip()).values())),_lines))
            if len(ns) < 1:
                raise Exception("Constraint doesn't match any data in the database")
            _ns = list(map(lambda x: x[0],ns))
            for line,d in _lines:
                if line in _ns:
                    __d = json.loads(d)
                    __d.update(data)
                    print(__d)
                    final_list_to_be_written.append(json.dumps(__d)+"\n")
                else:
                    final_list_to_be_written.append(d)
            f.writelines(final_list_to_be_written)
            valid_data = list(filter(lambda x: x != "\n",final_list_to_be_written))
            self.all_data = list(map(self.get_with_id,valid_data))
            print(self.all_data)
            if view:
                self.view()



# LOCAL SESSION               

def max_age_decorator(func):
    @wraps(func)
    def wrapper(*args,**kwargs):
         with open('session.db','r') as session_file:
            session_data = session_file.read()
            return session_data
    return wrapper

@dataclass
class LocalSession:
    max_age:int = field(default=None)

    def __init__(self,max_age:int):
        with open('session.db','w') as session_file:
            self.session_filename = 'session.db'
        pass

    @classmethod
    def get_data(cls):
        with open('session.db','r') as session_file:
            session_data = session_file.read()
            return session_data

    @classmethod
    def set_data(cls,data:str):
         with open('session.db','w') as session_file:
            session_file.write(data)


    @classmethod
    def remove_data(cls):
         with open('session.db','w') as session_file:
            session_file.truncate()



if __name__ == "__main__":
    db = Collection("students",["firstname","lastname","regno","year"],collection_folder = "MyCollectionFolder")
    for i in range(20):
       db.add_record({"firstname":"Hannah"+str(i),"lastname":"Williams","regno":48075,"year":i + 1})
    r = db.fetch_all()(view = True)
    print(r)    
    # db.delete_record(constraint = {"firstname":"Hannah"},view = True)