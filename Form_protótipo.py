import sqlite3
import pandas as pd


with open('ITM-22-111-Am-05-CBI22-077-Form_Prototipo-_Final-05-01-23_.csv', 'r') as arquivo_csv:

    # Lê o arquivo CSV
    leitor_csv = pd.read_csv('ITM-22-111-Am-05-CBI22-077-Form_Prototipo-_Final-05-01-23_.csv', sep=',', on_bad_lines='skip', low_memory=False)
    leitor_csv['Current'] = pd.to_numeric(leitor_csv['Current'], errors='coerce')
    leitor_csv['Voltage'] = pd.to_numeric(leitor_csv['Voltage'], errors='coerce')
    leitor_csv['Step'] = pd.to_numeric(leitor_csv['Step'], errors='coerce')
    leitor_csv['AhCha'] = pd.to_numeric(leitor_csv['AhCha'], errors='coerce')
    leitor_csv['AhDch'] = pd.to_numeric(leitor_csv['AhDch'], errors='coerce')
    leitor_csv['AhStep'] = pd.to_numeric(leitor_csv['AhStep'], errors='coerce')
    leitor_csv['AhAccu'] = pd.to_numeric(leitor_csv['AhAccu'], errors='coerce')
    leitor_csv['WhAccu'] = pd.to_numeric(leitor_csv['WhAccu'], errors='coerce')
    leitor_csv['WhCha'] = pd.to_numeric(leitor_csv['WhCha'], errors='coerce')
    leitor_csv['WhDch'] = pd.to_numeric(leitor_csv['WhDch'], errors='coerce')
    leitor_csv['WhStep'] = pd.to_numeric(leitor_csv['WhStep'], errors='coerce')
    leitor_csv = leitor_csv.fillna(0)
    p = leitor_csv.groupby('Step').last()




conexao = sqlite3.connect('Form_Protótipo_AM05.db')

conexao.execute('''CREATE TABLE IF NOT EXISTS am05
                     (Time_Stamp TEXT, Status TEXT, Prog_Time TEXT, Step_Time TEXT, Cycle TEXT, Cycle_Level TEXT, Procedure TEXT, Voltage REAL, Current REAL, AhCha REAL, AhDch REAL, AhStep TEXT, AhAccu REAL, WhAccu REAL, WhCha REAL, WhDch REAL, WhStep TEXT)''')

for linha in p.itertuples(index=False):
    Time_Stamp = linha[2]
    Status = linha[3]
    Prog_Time = linha[4]
    Step_Time = linha[5]
    Cycle = linha[6]
    Cycle_Level = linha[7]
    Procedure = linha[8]
    Voltage = linha[9]
    Current = linha[10]
    AhCha = linha[11]
    AhDch = linha[12]
    AhStep = linha[13]
    AhAccu = linha[14]
    WhAccu = linha[15]
    WhCha = linha[16]
    WhDch = linha[17]
    WhStep = linha[18]



conexao.execute("INSERT INTO am05 (Time_Stamp, Status, Prog_Time, Step_Time, Cycle, Cycle_Level, Procedure, Voltage, Current, AhCha, AhDch, AhStep, AhAccu, WhAccu, WhCha, WhDch, WhStep) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                (Time_Stamp, Status, Prog_Time, Step_Time, Cycle, Cycle_Level, Procedure, Voltage, Current, AhCha, AhDch, AhStep, AhAccu, WhAccu, WhCha, WhDch, WhStep))



conexao.commit()


conexao.execute("DELETE FROM am05 WHERE Voltage = 0;")
conexao.execute("DELETE FROM am05 WHERE ROWID = 14;")
conexao.execute("DELETE FROM am05 WHERE ROWID = 15;") 


conexao.commit()

conexao.close()
