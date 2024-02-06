import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import pyodbc

import paths_manager


def get_all_betas_from_excel_file():
    # MAKE SURE THAT THE UNDERLYING ASSETS ARE IN ORDER AND THAT THERE IS A COLUMN NAMED "valores"
    suby_betas = pd.read_excel("betas.xlsx")
    return suby_betas['valores'].tolist()

def export_excel_with_list_of_underlying_assets(list_of_underlying_assets):
    df = pd.DataFrame()
    df['tickers'] = list_of_underlying_assets
    df.to_excel('tickers_that_need_betas.xlsx')

def last_day_of_month(any_day):
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return next_month - datetime.timedelta(days=next_month.day)


paths_info = {
    'MONITOR.BDPRODUCTOS': ['Base de Datos', '5 - Bases de Datos/PRODUCTOS.accdb', 'FILE'],
}

paths = paths_manager.get_paths(paths_info)
db_file = paths["MONITOR.BDPRODUCTOS"]

conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};'fr'DBQ={db_file};')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding='latin-1')
cursor = conn.cursor()

today = datetime.datetime(2024, 2, 2).replace(hour=0, minute=0, second=0, microsecond=0)
next_12_monthiversaries_of_today = [today + relativedelta(months=i) for i in range(12)]
next_12_ends_of_the_month = [last_day_of_month(day) for day in next_12_monthiversaries_of_today]
print(today)
print(next_12_monthiversaries_of_today)
print(next_12_ends_of_the_month)

cursor.execute(
    """
    SELECT fecha FROM POSITIONS ORDER BY FECHA DESC
    """
)

last_date_available = cursor.fetchall()[0][0]
print(last_date_available)

cursor.execute(
    """
    SELECT a.ZestID, p.Quantity, s.Subyacente, s.PrecioRef, f.Fecha, a.Kcapital
    FROM ((T_AUTOCALL a
    INNER JOIN POSITIONS p ON a.ZestID = p.ZestID)
    INNER JOIN REL_NOTASUBYAC s ON a.ZestID = s.ZestID)
    INNER JOIN FECHAS f ON a.ZestID = f.ZestID
    WHERE a.FinalValueDate >= ?
    AND a.ISSUEDATE <= ?
    AND (a.FECHAAUTOCALL IS NULL OR a.FECHAAUTOCALL >= ?)
    AND s.FechaSalida IS NULL
    AND p.FECHA = ?
    AND a.Kcapital IS NOT NULL
    ORDER BY a.ZestID;
    """,
    (today, today, today, last_date_available)
)

active_notes = cursor.fetchall()
note_dict = {}

for note in active_notes:
    if note[0] not in note_dict.keys():
        obs_dates_strs = note[4].split(', ')
        obs_dates = [datetime.datetime.strptime(date, '%Y-%m-%d') for date in obs_dates_strs]
        note_dict[note[0]] = [note[1], [note[2]], [note[3]], obs_dates, note[5]]
    else:
        note_dict[note[0]][1].append(note[2])
        note_dict[note[0]][2].append(note[3])

for k, v in note_dict.items():
    print(f"NOTE {k} #####################################################################")
    for value in v:
        print(value)

sp_500_rets = [x / 100 for x in range(-30, 35, 5)]

subys = list(set([x[2] for x in active_notes]))
subys.sort()
print(subys)

suby_prices = []
for suby in subys:
    cursor.execute(
        """
        SELECT Precio FROM PRECIOS_HISTORICO
        WHERE Ticker = ? AND Fecha = ?
        """,
        (suby, today)
    )
    suby_prices.append(cursor.fetchall()[0][0])
print(suby_prices)

betas = get_all_betas_from_excel_file()

suby_forecasted_prices_df = pd.DataFrame()
pd.options.display.float_format = '{:,.2f}'.format
suby_forecasted_prices_df['Escenarios'] = sp_500_rets

for end_of_the_month in next_12_ends_of_the_month:
    accum_amounts = []
    for sp_500_ret in sp_500_rets:
        accum_amount = 0
        suby_prices_in_t = [suby_prices[i] * (1 + (sp_500_ret * betas[i])) for i in range(len(suby_prices))]
        print(f"SP 500 RETURN IS {sp_500_ret} _________________________________________________________________________________________")

        for note in note_dict.keys():
            print(f"NOTE {note} ###########################################################################################")
            print(f"SUBYS: {note_dict[note][1]}")
            strike_prices = note_dict[note][2]
            print(strike_prices)
            these_suby_prices = [suby_prices[subys.index(x)] for x in note_dict[note][1]]
            these_suby_prices_in_t = [suby_prices_in_t[subys.index(x)] for x in note_dict[note][1]]
            print("betas")
            print([betas[subys.index(x)] for x in note_dict[note][1]])
            for obs_date in [x for x in note_dict[note][3] if x >= today and x <= end_of_the_month]:
                print(obs_date)
                suby_prices_in_this_obs_date = [these_suby_prices[i] + (((these_suby_prices_in_t[i] - these_suby_prices[i]) / (end_of_the_month - today).days) * (obs_date - today).days) for i in range(len(these_suby_prices))]
                print(suby_prices_in_this_obs_date)
                print("Levels regarding the strike prices")
                levels = [suby_prices_in_this_obs_date[i] / strike_prices[i] for i in range(len(suby_prices_in_this_obs_date))]
                print(levels)
                maturity_date = note_dict[note][3][-1]
                nominal_value = note_dict[note][0]
                if obs_date == maturity_date:
                    capital_barrier = note_dict[note][4]
                    if min(levels) >= capital_barrier:
                        accum_amount += nominal_value
                    else:
                        accum_amount += nominal_value * min(levels)

                if min(levels) >= 1:
                    accum_amount += nominal_value
                    print("AUTOCALL")
                    break
        accum_amounts.append(accum_amount)
    end_of_the_month_str = end_of_the_month.strftime("%Y-%m-%d")
    suby_forecasted_prices_df[f"Al {end_of_the_month_str}"] = accum_amounts

print(f"REVOLVENTES (USD)")
print(suby_forecasted_prices_df.to_string())
