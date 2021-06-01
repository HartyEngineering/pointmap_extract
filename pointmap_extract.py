import pyodbc
import pandas as pd

def sql_query(sql):
    
    #database must be updated to use.
    database = 'database_name_goes_here'
    connection = pyodbc.connect('DSN='+database)
    crsr = connection.cursor()
    #print(sql)
    crsr.execute(sql)
    ans = crsr.fetchall()
    data = pd.read_sql(sql,connection)  
    connection.close()
    return(data)

def analog_input(tagName):
    sql = "SELECT [anain.type],[anain.iospec.external],description,name,units "\
          "FROM analog "\
          "WHERE (name LIKE '%"+tagName+"%' AND pointtype='telemetered' AND [group@raw]!=13) "\
          "ORDER BY [anain.iospec.external];"
    
    data = sql_query(sql)
    data = data.rename(columns = {"anain.type":"Type",
                                  "anain.iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname"})
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = 300000+data['Addr']+1
    
    #run the rate input and totalizer functions and get return
    rate_data = rate_input(tagName)
    acc_data = acc_input(tagName)
    
    result = data.append([rate_data,acc_data])
    result = result.sort_values(by=['Gateway'])
    
    final_result = analog_input_sort(result)
    
    return(final_result)

#used in analog_input
def rate_input(tagName):
    sql = "SELECT [anain.type],[anain.iospec.external],description,name,runits "\
          "FROM rate "\
          "WHERE (name LIKE '%"+tagName+"%' AND pointtype='telemetered' AND [group@raw]!=13) "\
          "ORDER BY [anain.iospec.external];"
    
    data = sql_query(sql)
    data = data.rename(columns = {"anain.type":"Type",
                                  "anain.iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname",
                                  "runits":"units"})
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = 300000+data['Addr']+1
    return(data)

#used in analog input
def acc_input(tagName):
    sql = "SELECT [accin.type],[accin.iospec.external],description,name,dunits "\
          "FROM rate "\
          "WHERE (name LIKE '%"+tagName+"%' AND pointtype='telemetered' AND [group@raw]!=13) "\
          "ORDER BY [accin.iospec.external];"
    
    data = sql_query(sql)
    data = data.rename(columns = {"accin.type":"Type",
                                  "accin.iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname",
                                  "dunits":"units"})
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = 300000+data['Addr']+1
    return(data)

def analog_output(tagName):
    sql = "SELECT [otype],[anaout.iospec.external],description,name,units "\
          "FROM analog "\
          "WHERE (name LIKE '%"+tagName+"%' AND pointtype='telemetered' AND [output@raw] = 1 AND [group@raw]!=13) "\
          "ORDER BY [anaout.iospec.external];"
    data = sql_query(sql)
    data = data.rename(columns = {"otype":"Type",
                                  "anaout.iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname"})
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = 400000+data['Addr']+1
    
    result = analog_output_sort(data)
    return(result)

def status_input1(tagName):
    sql = "SELECT \"inbit.bitdef[0].iospec.external\",description,name "\
          "FROM status "\
          "WHERE (name LIKE '%"+tagName+"%' AND pointtype='telemetered' AND [group@raw]!=13) "\
          "ORDER BY \"inbit.bitdef[0].iospec.external\";"
    #print(sql)
    data = sql_query(sql)
    data = data.rename(columns = {"inbit.bitdef[0].iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname"})
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = 100000+data['Addr']+1
    
    #run the status_input2 function gets the bit[2] values
    status_inbit2_data = status_input2(tagName)
    result = data.append([status_inbit2_data])
    
    
    result = result.sort_values(by=['Gateway'])
    result = result.reset_index(drop=True)
    
    final_result = status_input_sort(result)
    
    return(final_result)

def status_input2(tagName):
    sql = "SELECT \"inbit.bitdef[1].iospec.external\",description,name "\
          "FROM status "\
          "WHERE (name LIKE '%"+tagName+"%' AND pointtype='telemetered' AND [inbit.bitwi]=2 AND [group@raw]!=13) "\
          "ORDER BY \"inbit.bitdef[1].iospec.external\";"
    #print(sql)
    data = sql_query(sql)
    data = data.rename(columns = {"inbit.bitdef[1].iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname"})
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = 100000+data['Addr']+1
    
    
    return(data)

def status_output1(tagName):
    sql = "SELECT \"outs[1].iospec.external\",description,name "\
          "FROM status "\
          "WHERE (name LIKE '%"+tagName+"%' AND \"outs[1].iospec.external\" LIKE '%FC5%' AND pointtype='telemetered' AND [group@raw]!=13) "\
          "ORDER BY \"outs[1].iospec.external\";"

    data = sql_query(sql)
    data = data.rename(columns = {"outs[1].iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname"})
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = data['Addr']+1
    
    status_outs2_data = status_output2(tagName)
    result = data.append([status_outs2_data])
    
    
    result = result.sort_values(by=['Gateway'])
    result = result.reset_index(drop=True)

    final_result = status_output_sort(result)
    
    return(final_result)

def status_output2(tagName):
    sql = "SELECT \"outs[2].iospec.external\",description,name "\
          "FROM status "\
          "WHERE (name LIKE '%"+tagName+"%' AND \"outs[2].iospec.external\" LIKE '%FC5%' AND pointtype='telemetered' AND [group@raw]!=13) "\
          "ORDER BY \"outs[2].iospec.external\";"
 
    data = sql_query(sql)
    data = data.rename(columns = {"outs[2].iospec.external":"SCADA_Addr",
                                  "description":"Description",
                                  "name":"Tagname"})
    
    data['Addr'] = data['SCADA_Addr'].str.split(":",2).str[1]
    data['Addr'] = pd.to_numeric(data['Addr'],downcast='integer')
    data['Gateway'] = data['Addr']+1
    

    return(data)

def analog_input_sort(pd_analogInput):
    #Analog INPUT
    #takes the output data from database and convert to pointmap friendly layout
    #The while loop helps it fill in the blanks while it looks for points

    #Initializes the Analog Input sheet and columns
    analogInput = pd.DataFrame(columns = ['PlcAddr','PlcAlias','Type','Gateway','SCADA_Addr','Description','Tagname','units'])

    startu16int = 0
    startfloat = 1000
    start32int = 2000


    count=0

    for row in pd_analogInput.itertuples():
        while True:

    #for unsigned 16 bit integers
            if row.Type == 'unsign 16 int' or row.Type == 'signed 16 int':
                if row.Addr == startu16int:
                    dic = {'Type':row.Type,
                           'SCADA_Addr':row.SCADA_Addr,
                           'Gateway':row.Gateway,
                           'Description':row.Description,
                           'Tagname':row.Tagname,
                           'units':row.units}

                    analogInput = analogInput.append(dic, ignore_index=True)
                    startu16int = startu16int+1
                    break
                else:
                    address = "FC4:"+str(startu16int)
                    gate = 300000+startu16int+1


                    dic = {'SCADA_Addr':address,'Gateway':gate, 'Type':row.Type}
                    analogInput = analogInput.append(dic, ignore_index=True)
                    startu16int = startu16int + 1
                    continue

    #For Floats
            elif row.Type == 'float':
                #print(row.Addr)
                #print(startfloat)
                if row.Addr == startfloat:
                    #print("1")
                    dic = {'Type':row.Type,
                           'SCADA_Addr':row.SCADA_Addr,
                           'Gateway':row.Gateway,
                           'Description':row.Description,
                           'Tagname':row.Tagname,
                           'units':row.units}
                    analogInput = analogInput.append(dic, ignore_index=True)
                    startfloat = startfloat + 2
                    break

                else:
                    #print("2")
                    address = "FC4:"+str(startfloat)
                    gate = 300000+startfloat+1
                    dic = {'SCADA_Addr':address,'Gateway':gate, 'Type':row.Type}
                    analogInput = analogInput.append(dic, ignore_index=True)
                    startfloat = startfloat + 2
                    continue

    #For 32bit Integers        
            elif row.Type == 'unsign 32 int' or row.Type == 'signed 32 int':
                if row.Addr == start32int:
                    #print("1")
                    dic = {'Type':row.Type,
                           'SCADA_Addr':row.SCADA_Addr,
                           'Gateway':row.Gateway,
                           'Description':row.Description,
                           'Tagname':row.Tagname,
                           'units':row.units}
                    analogInput = analogInput.append(dic, ignore_index=True)
                    start32int = start32int + 2
                    break

                else:
                    #print("2")
                    address = "FC4:"+str(start32int)
                    gate = 300000+start32int+1


                    dic = {'SCADA_Addr':address,'Gateway':gate, 'Type':row.Type}
                    analogInput = analogInput.append(dic, ignore_index=True)
                    start32int = start32int + 2
                    continue

            #else:
                #print("error")
                #break
    analogInput = analogInput.sort_values(by=['Gateway'])
    return(analogInput)


def analog_output_sort(pd_analogOutput):
    #Analog Output
    #takes the output data from database and convert to pointmap friendly layout
    #The while loop helps it fill in the blanks while it looks for points

    #Initializes the Analog Output sheet and columns
    analogOutput = pd.DataFrame(columns = ['PlcAddr','PlcAlias','Type','Gateway','SCADA_Addr','Description','Tagname','units'])

    startu16int = 0
    startfloat = 1000
    start32int = 2000


    count=0

    for row in pd_analogOutput.itertuples():
        while True:

    #for unsigned 16 bit integers
            if row.Type == 'unsign 16 int' or row.Type == 'signed 16 int':
                if row.Addr == startu16int:
                    dic = {'Type':row.Type,
                           'SCADA_Addr':row.SCADA_Addr,
                           'Gateway':row.Gateway,
                           'Description':row.Description,
                           'Tagname':row.Tagname,
                           'units':row.units}

                    analogOutput = analogOutput.append(dic, ignore_index=True)
                    startu16int = startu16int+1
                    break
                else:
                    address = "FC6:"+str(startu16int)
                    gate = 400000+startu16int+1


                    dic = {'SCADA_Addr':address,'Gateway':gate, 'Type':row.Type}
                    analogOutput = analogOutput.append(dic, ignore_index=True)
                    startu16int = startu16int + 1
                    continue

    #For Floats
            elif row.Type == 'float':
                if row.Addr == startfloat:
                    #print("1")
                    dic = {'Type':row.Type,
                           'SCADA_Addr':row.SCADA_Addr,
                           'Gateway':row.Gateway,
                           'Description':row.Description,
                           'Tagname':row.Tagname,
                           'units':row.units}
                    analogOutput = analogOutput.append(dic, ignore_index=True)
                    startfloat = startfloat + 2
                    break

                else:
                    #print("2")
                    address = "FC16:"+str(startfloat)
                    gate = 400000+startfloat+1


                    dic = {'SCADA_Addr':address,'Gateway':gate, 'Type':row.Type}
                    analogOutput = analogOutput.append(dic, ignore_index=True)
                    startfloat = startfloat + 2
                    continue

    #For 32bit Integers        
            elif row.Type == 'unsign 32 int' or row.Type == 'signed 32 int':

                if row.Addr == start32int:
                    #print("1")
                    dic = {'Type':row.Type,
                           'SCADA_Addr':row.SCADA_Addr,
                           'Gateway':row.Gateway,
                           'Description':row.Description,
                           'Tagname':row.Tagname,
                           'units':row.units}
                    analogOutput = analogOutput.append(dic, ignore_index=True)
                    start32int = start32int + 2
                    break

                else:
                    #print("2")
                    address = "FC16:"+str(start32int)
                    gate = 400000+start32int+1


                    dic = {'SCADA_Addr':address,'Gateway':gate, 'Type':row.Type}
                    analogOutput = analogOutput.append(dic, ignore_index=True)
                    start32int = start32int + 2
                    continue

            else:
                print("error")
                break


    analogOutput = analogOutput.sort_values(by=['Gateway'])
    return(analogOutput)
    
    
def status_input_sort(pd_statusInput):
    #status INPUT
    #takes the output data from database and convert to pointmap friendly layout
    #The while loop helps it fill in the blanks while it looks for points

    #Initializes the Status Input sheet and columns
    statusInput = pd.DataFrame(columns = ['PlcAddr','PlcAlias','Gateway','SCADA_Addr','Description','Tagname'])

    startbit = 0

    count=0

    for row in pd_statusInput.itertuples():
        while True:

    #for status bits
            if row.Addr == startbit:
                #print(f'{row.Addr} = {startbit}')
                scada_address = row.SCADA_Addr
                scada_address_str = str(scada_address).zfill(4)

                gate_address = int(row.Gateway)
                gate_address_str = str(gate_address).zfill(6)

                dic = {'SCADA_Addr':scada_address_str,
                       'Gateway':gate_address_str,
                       'Description':row.Description,
                       'Tagname':row.Tagname}

                statusInput = statusInput.append(dic, ignore_index=True)
                startbit = startbit+1
                break
            
            #elif the there is a duplicate reference to a tag address
            elif row.Addr == startbit-1:
                #print(f'{row.Addr} = {startbit} minus 1')
                scada_address = row.SCADA_Addr
                scada_address_str = str(scada_address).zfill(4)

                gate_address = int(row.Gateway)
                gate_address_str = str(gate_address).zfill(6)

                dic = {'SCADA_Addr':scada_address_str,
                       'Gateway':gate_address_str,
                       'Description':row.Description,
                       'Tagname':row.Tagname}

                statusInput = statusInput.append(dic, ignore_index=True)
                startbit = startbit
                
                print(f'duplicate at FC2:{str(row.Addr).zfill(4)}')
                break
            else:

                address = "FC2:"+str(startbit).zfill(4)

                gate = 100000+startbit+1
                gatestr = str(gate)
                gatestr = gatestr.zfill(6)

                dic = {'SCADA_Addr':address,'Gateway':gatestr}
                statusInput = statusInput.append(dic, ignore_index=True)
                startbit = startbit + 1
                continue


    statusInput = statusInput.sort_values(by=['Gateway'])
    return(statusInput)

def status_output_sort(pd_statusOutput):
    #status output
    #takes the output data from database and convert to pointmap friendly layout
    #The while loop helps it fill in the blanks while it looks for points

    #Initializes the Status Input sheet and columns
    statusOutput = pd.DataFrame(columns = ['PlcAddr','PlcAlias','Gateway','SCADA_Addr','Description','Tagname'])

    startbit = 0
    count=0

    for row in pd_statusOutput.itertuples():
        while True:

    #for status bits
            if row.Addr == startbit:
                #print(f'{row.Addr} = {startbit}')
                scada_address = row.SCADA_Addr
                scada_address_str = str(scada_address).zfill(4)

                gate_address = int(row.Gateway)
                gate_address_str = str(gate_address).zfill(6)

                dic = {'SCADA_Addr':scada_address_str,
                       'Gateway':gate_address_str,
                       'Description':row.Description,
                       'Tagname':row.Tagname}

                statusOutput = statusOutput.append(dic, ignore_index=True)
                startbit = startbit+1
                break
    
            #elif the there is a duplicate reference to a tag address
            elif row.Addr == startbit-1:
                #print(f'{row.Addr} = {startbit} minus 1')
                scada_address = row.SCADA_Addr
                scada_address_str = str(scada_address).zfill(4)

                gate_address = int(row.Gateway)
                gate_address_str = str(gate_address).zfill(6)

                dic = {'SCADA_Addr':scada_address_str,
                       'Gateway':gate_address_str,
                       'Description':row.Description,
                       'Tagname':row.Tagname}

                statusOutput = statusOutput.append(dic, ignore_index=True)
                startbit = startbit
                print(f'duplicate at FC5:{str(startbit).zfill(4)}')
                break
            else:

                address = "FC5:"+str(row.Addr).zfill(4)

                gate = startbit+1
                gatestr = str(gate)
                gatestr = gatestr.zfill(6)

                dic = {'SCADA_Addr':address,'Gateway':gatestr}
                statusOutput = statusOutput.append(dic, ignore_index=True)
                startbit = startbit + 1
                continue


    statusOutput = statusOutput.sort_values(by=['Gateway'])
    return(statusOutput)

def pointmap(site):
    statusInput = status_input1(site)
    statusOutput = status_output1(site)
    
    analogOutput = analog_output(site)
    #analogOutput.head(10)
    analogInput = analog_input(site)

    
    
    try:
        with pd.ExcelWriter('pointmap.xlsx') as writer:
            analogInput.to_excel(writer,sheet_name='Analog Inputs', index=False)  
            analogOutput.to_excel(writer,sheet_name='Analog Outputs', index=False)
            statusInput.to_excel(writer,sheet_name='Digital Inputs', index=False)
            statusOutput.to_excel(writer,sheet_name='Digital Outputs', index=False)
    except:
        print(f'Error saving file. Verify file is not open.')
        
    return(print(f'Pointmap exported'))



pointmap("enter remote name here.")