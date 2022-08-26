from api import Dadata
import pandas as pd
from datetime import datetime
import sys, os, traceback, anyio

#==============================================================================#

APIKEY = 'e2a8b90b8266905a8a4ca458f282257d635b1c7a'
APISEC = 'c98540239c753f10ca3ce5e1b93b0c5e405c4dda'
COLS = {'value': 'Наименование', 'full_name': 'Полное наименование', 'type': 'Тип юрлица', 'opf': 'Организационно-правовая форма',
        'smb': 'Категория в реестре малого и среднего предпринимательства', 'address': 'Адрес', 'coords': 'Координаты адреса',
        'phones': 'Телефонные номера', 'emails': 'Адреса электронной почты',
        'kpp': 'КПП', 'inn': 'ИНН', 'ogrn': 'ОГРН', 'okato': 'Код ОКАТО', 'oktmo': 'Код ОКТМО', 
        'okpo': 'Код ОКПО', 'okogu': 'Код ОКОГУ', 'okfs': 'Код ОКФС', 'okveds': 'Коды ОКВЭД',
        'manager_name': 'ФИО руководителя', 'manager_post': 'Должность руководителя', 'founders': 'Учредители', 
        'branch_type': 'Тип подразделения', 'branch_count': 'Количество филиалов',
        'state': 'Статус', 'registration_date': 'Дата регистрации', 'ogrn_date': 'Дата внесения в ОГРН',
        'capital': 'Капитал (руб.)', 'income': 'Доход (руб.)', 'expense': 'Расход (руб.)', 'debt': 'Недоимки по налогам (руб.)',
        'penalty': 'Штрафы по налогам (руб.)', 'fin_year': 'Год финансовой отчётности'}

#==============================================================================#

def get_smb(data):
    docs = data.get('documents', None)
    if not docs: return ''
    smb = docs.get('smb', None)
    if not smb: return ''
    cat = smb.get('category', None)
    if not cat: return ''
    if cat == 'MICRO':
        return 'Микробизнес'
    if cat == 'SMALL':
        return 'Малый бизнес'
    if cat == 'MEDIUM':
        return 'Средний бизнес'
    return ''

def get_phones(data, key='phones'):
    phones = data.get(key, None)
    if not phones: return ''
    out = []
    for phone in phones:
        phone_data = phone.get('data', None)
        if not phone_data:
            out.append(phone['value'])
            continue
        contact = phone_data.get('contact', None)
        if contact:
            out.append(f"{phone_data['source']} ({contact})")
        else:
            out.append(phone_data['source'])
    return ', '.join(out) if out else ''

def get_okved(data):
    okveds = data.get('okveds', None)
    if not okveds:
        return data.get('okved', '')
    return ', '.join(f"{okved['code']} ({okved['name']})" for okved in okveds)

def get_manager_name(data):
    management = data.get('management', None)
    if data['type'] == 'INDIVIDUAL':
        try:
            return ' '.join((data['fio'].get('surname', ''), data['fio'].get('name', ''), data['fio'].get('patronymic', ''))).strip()
        except:
            return management.get('name', '') if management else ''
    return management.get('name', '') if management else ''

def get_founders(data):
    founders = data.get('founders', None)
    if not founders: return ''
    return ', '.join(f"{founder['name']}{(' (' + founder['share']['value'] + '%)') if 'share' in founder and founder['share']['value'] else ''}" for founder in founders)

def get_state(data):
    state = data.get('state', None)
    if not state: return ''
    if state['status'] == 'ACTIVE':
        return 'Действующая'
    elif state['status'] == 'LIQUIDATING':
        return 'Ликвидируется'
    elif state['status'] == 'LIQUIDATED':
        return 'Ликвидирована'
    elif state['status'] == 'BANKRUPT':
        return 'Банкротство'
    elif state['status'] == 'REORGANIZING':
        return 'В процессе присоединения к другому юрлицу'
    return ''

def get_date(data, key, formatstr=r'%Y-%m-%d'):
    date_ = data.get(key, None)
    if not date_: return ''
    date_ = int(date_) / 1000
    return datetime.utcfromtimestamp(date_).strftime(formatstr)

def get_capital(data):
    capital = data.get('capital', None)
    if not capital: return ''
    return f"{capital['value']} ({capital['type']})"

#==============================================================================#

def process_result(data):
    finance = data.get('finance', None)
    out = {'value': data['value'], 
           'full_name': data['name'].get('full_with_opf', ''), 
           'type': 'Юридическое лицо' if data['type'] == 'LEGAL' else 'ИП',
           'opf': f"{data['opf']['short']} ({data['opf']['full']})",
           'smb': get_smb(data),
           'address': data['address']['value'],
           'coords': '',
           'phones': get_phones(data),
           'emails': get_phones(data, 'emails'),
           'kpp': data.get('kpp', ''), 'inn': data.get('inn', ''), 'ogrn': data.get('ogrn', ''), 
           'okato': data.get('okato', ''), 'oktmo': data.get('oktmo', ''),
           'okpo': data.get('okpo', ''), 'okogu': data.get('okogu', ''), 'okfs': data.get('okfs', ''), 
           'okveds': get_okved(data),
           'manager_name': get_manager_name(data),
           'manager_post': data['management'].get('post', '') if data.get('management', None) else '',
           'founders': get_founders(data),
           'branch_type': 'Головная организация' if data['branch_type'] == 'MAIN' else 'Филиал',
           'branch_count': data['branch_count'],
           'state': get_state(data),
           'registration_date': get_date(data['state'], 'registration_date'),
           'ogrn_date':  get_date(data, 'ogrn_date'),
           'capital': get_capital(data),
           'income': finance.get('income', '') if finance else '',
           'expense': finance.get('expense', '') if finance else '',
           'debt': finance.get('debt', '') if finance else '',
           'penalty': finance.get('penalty', '') if finance else '',
           'year': finance.get('year', '') if finance else ''
    }
    return out

#==============================================================================#    

async def main():
    fname = 'companies.txt'
    if len(sys.argv) > 1:
        fname = sys.argv[1]
    fname = os.path.abspath(fname)
    if not os.path.isfile(fname):
        print('Необходимо передать имя файла со списком компаний или создать файл "companies.txt" в корне программы!', file=sys.stderr)
        return

    try:
        with open(fname, 'r', encoding='utf-8') as f:
            companies = [comp.strip() for comp in f]

        records = []

        async with Dadata(APIKEY, APISEC) as client:
            for company in companies:
                # print(f'"{company}"')   
                res = await client.company(company, single=True, status=['ACTIVE'], type='LEGAL')
                if not res:
                    records.append({k: (company if k == 'value' else '') for k in COLS.keys()})
                    continue
                res = res['data'] | {'value': res['value']}
                records.append(process_result(res))

        df = pd.DataFrame.from_records(records, columns=list(COLS.keys()))
        df.rename(columns=COLS, inplace=True)
        df.to_excel('companies.xlsx', index=False, engine='openpyxl', encoding='utf-8')
        print('Данные сохранены в файл "companies.xlsx"!')

    except:
        traceback.print_exc(limit=5, file=sys.stderr)

#==============================================================================#

if __name__ == '__main__':
    anyio.run(main)