import time
import datetime as dt
import glob
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class CRMExtractor:

    def __init__(self, login, password, url, path, start_date=None, end_date=None):
        self.login = login
        self.password = password 
        self.url = url
        if not start_date:
            self.start_date = dt.date.today()
        else:
            self.start_date = start_date
        if not end_date:  
            self.end_date = dt.date.today()
        else:
            self.end_date = end_date
        self.path = path

        # Создание объекта опций Chrome
        self.chrome_options = Options()

        # Добавление настроек браузера
        self.chrome_options.add_argument("--window-size=1920,1080")
        self.chrome_options.add_argument("--disable-notifications")              # Отключение всплывающих уведомлений в браузере
        self.chrome_options.add_argument('--verbose')                            # Уровень журнала. --verbose эквивалентно --log-level=ALL и --silent эквивалентно --log-level=OFF
        self.chrome_options.add_argument("--disable-extensions")                 # позволяет отключить все расширения браузера при запуске
        self.chrome_options.add_argument("--start-maximized")                    # Запуск с развернутым на весь экран окном
        self.chrome_options.add_argument('--headless')                           # Headless Browser - это веб-браузер без графического пользовательского интерфейса (GUI)
        self.chrome_options.add_argument('--disable-gpu')                        
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--ignore-certificate-errors')
        self.chrome_options.add_argument('--disable-software-rasterizer')
        self.chrome_options.add_argument('--allow-running-insecure-content')
        self.chrome_options.add_experimental_option("prefs", {
            "download.default_directory": path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False,
        })        

        # Инициализация драйвера Chrome с использованием опций и сервиса
        self.driver = webdriver.Chrome(
            options=self.chrome_options
        )
        self.driver.delete_all_cookies()

    def auth(self):
        # Открытие веб-страницы в браузере
        self.driver.get(self.url)
        # Заполнение формы входа
        print('Логинюсь')
        login_field = self.driver.find_element(By.NAME, 'username')
        login_field.send_keys(self.login)
        password_field = self.driver.find_element(By.NAME, 'password')
        password_field.send_keys(self.password)
        confirm_button = self.driver.find_element(By.NAME, 'login')
        confirm_button.click()

    def get_requests(self, division=None):
        self.auth()
        # Ожидание загрузки страницы и появления элемента
        wait = WebDriverWait(self.driver, 20)
        menu_item = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, 'Процесс продаж'))
        )
        # Выбор нужного отчета
        print('Выбираю в меню нужный отчет (ОБРАЩЕНИЯ)')
        actions = ActionChains(self.driver)
        # Перемещение курсора к указанному элементу
        actions.move_to_element(menu_item).perform()
        menu_item = self.driver.find_element(By.LINK_TEXT, 'Обращения')
        print("Адрес ссылки:", menu_item.get_attribute("href"))
        menu_item.click()

        # Ожидание загрузки страницы и появления элемента шестеренки
        wait = WebDriverWait(self.driver, 10)
        element = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="request-grid"]/div[1]/div[1]/button/i'
            ))
        )
        element.click()

        print('Добавляю поля в выгрузку')
        # Добавление полей в выгрузку
        for _ in range(1,11):
            try:
                field_item = wait.until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        f'//*[@id="modal_customizable"]/div/div/div[2]/div/form/fieldset/div/div[1]/select/option[1]'
                    ))
                )
                field_item.click()
                button = self.driver.find_element(
                    By.XPATH,
                    '//*[@id="modal_customizable"]/div/div/div[2]/div/form/fieldset/div/div[2]/a[1]'
                )
                button.click()
            except:
                break       
        
        ok_button = self.driver.find_element(
            By.XPATH,
            '//*[@id="modal_customizable"]/div/div/div[3]/button'
        )
        ok_button.click()

        # Выбираем ВСЕ ОБРАЩЕНИЕ(АРХИВ)
        print('Выбираем ВСЕ ОБРАЩЕНИЕ(АРХИВ)')
        menu_item = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="archive"]/a'))
        )
        menu_item.click()
        time.sleep(10)

        #Настройка отчета
        print('Разворачиваю настройки отчета')
        menu_item = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="grand_selector"]/div[1]/div/table[2]/tbody/tr/td[3]/a'
            ))
        )
        menu_item.click()

        # Если выгружаем заявки по BUS, то необходимо выбрать производителя
        if division:
            try:
                menu_item = wait.until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        '//*[@id="interval_type"]'          ### Исправить
                    ))
                )
                # Выбор элемента из выпадающего списка
                select = Select(menu_item)
                select.select_by_visible_text(division)
            except:
                raise Exception(
                    'Элемент для выбора производителя не найден. Возможно, данный элемент недоступен для данного аккаунта.'
                )           

        print('Выставляю тип выгрузки за месяц')
        menu_item = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="interval_type"]'
            ))
        )
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_visible_text("МС")

        print('Выставляю год начала периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="start_year"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.start_date.year))
        time.sleep(1)

        print('Выставляю месяц начала периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="counter_min"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.start_date.month))
        time.sleep(1)

        print('Выставляю год конца периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="end_year"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.end_date.year))
        time.sleep(1)

        print('Выставляю месяц конца периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="counter_max"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.end_date.month))
        time.sleep(1)

        # Перед скачиванием файла экселя очищаем целевую папку
        file_pattern = os.path.join(self.path, 'Obracsheniya*')
        matching_files = glob.glob(file_pattern)
        if matching_files:
            for file_path in matching_files:
                os.remove(file_path)
                print(f"Удален файл: {file_path}")
        else:
            print(f"Файлы, соответствующие шаблону имени 'Obracsheniya*', не найдены.")

        # Скачивание отчета в эксель
        print('Нажимаю кнопку')
        menu_item = self.driver.find_element(
            By.XPATH,
            '//*[@id="grand_selector"]/div[1]/div/table[2]/tbody/tr/td[6]/div/a'
        )
        self.driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
            menu_item
        )
        actions.move_to_element(menu_item).click().perform()
        self.ts = dt.datetime.now()

        self.file_check('Obracsheniya')

    def get_worklists(self, division=None):
        self.auth()
        # Ожидание загрузки страницы и появления элемента
        wait = WebDriverWait(self.driver, 20)
        menu_item = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, 'Процесс продаж'))
        )
        # Выбор нужного отчета
        print('Выбираю в меню нужный отчет (РАБОЧИЕ ЛИСТЫ)')
        actions = ActionChains(self.driver)
        # Перемещение курсора к указанному элементу
        actions.move_to_element(menu_item).perform()
        # menu_item = self.driver.find_element(By.LINK_TEXT, 'Рабочие лиcты')
        menu_item = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, 'Рабочие лиcты'))
        )
        print("Адрес ссылки:", menu_item.get_attribute("href"))
        menu_item.click()

        # Ожидание загрузки страницы и появления элемента шестеренки
        wait = WebDriverWait(self.driver, 10)
        element = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="worklists-grid"]/div[1]/div[1]/button'
            ))
        )
        element.click()

        print('Добавляю поля в выгрузку')
        # Добавление полей в выгрузку
        for _ in range(1,4):
            try:
                field_item = wait.until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        f'//*[@id="modal_customizable"]/div/div/div[2]/div/form/fieldset/div/div[1]/select/option[1]'
                    ))
                )
                field_item.click()
                button = self.driver.find_element(
                    By.XPATH,
                    '//*[@id="modal_customizable"]/div/div/div[2]/div/form/fieldset/div/div[2]/a[1]'
                )
                button.click()
            except:
                break       
        
        ok_button = self.driver.find_element(
            By.XPATH,
            '//*[@id="modal_customizable"]/div/div/div[3]/button'
        )
        ok_button.click()
        time.sleep(10)

        #Настройка отчета
        print('Разворачиваю настройки отчета')
        menu_item = wait.until(
            EC.visibility_of_element_located((
                By.XPATH,
                '//*[@id="grand_selector"]/div[1]/div/table[2]/tbody/tr/td[3]/a'
            ))
        )
        menu_item.click()

        # Если выгружаем заявки по BUS, то необходимо выбрать производителя
        if division:
            try:
                menu_item = wait.until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        '//*[@id="interval_type"]'          ### Исправить
                    ))
                )
                # Выбор элемента из выпадающего списка
                select = Select(menu_item)
                select.select_by_visible_text(division)
            except:
                raise Exception(
                    'Элемент для выбора производителя не найден. Возможно, данный элемент недоступен для данного аккаунта.'
                )           

        print('Выставляю тип выгрузки за месяц')
        menu_item = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="interval_type"]'
            ))
        )
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_visible_text("МС")

        print('Выставляю год начала периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="start_year"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.start_date.year))
        time.sleep(1)

        print('Выставляю месяц начала периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="counter_min"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.start_date.month))
        time.sleep(1)

        print('Выставляю год конца периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="end_year"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.end_date.year))
        time.sleep(1)

        print('Выставляю месяц конца периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="counter_max"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.end_date.month))
        time.sleep(1)

        # Перед скачиванием файла экселя очищаем целевую папку
        file_pattern = os.path.join(self.path, 'Rabochie_listy*')
        matching_files = glob.glob(file_pattern)
        if matching_files:
            for file_path in matching_files:
                os.remove(file_path)
                print(f"Удален файл: {file_path}")
        else:
            print(f"Файлы, соответствующие шаблону имени 'Rabochie_listy*', не найдены.")

        # Скачивание отчета в эксель
        print('Нажимаю кнопку')
        menu_item = self.driver.find_element(
            By.XPATH,
            '//*[@id="grand_selector"]/div[1]/div/table[2]/tbody/tr/td[6]/div/a'
        )
        self.driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
            menu_item
        )
        actions.move_to_element(menu_item).click().perform()
        self.ts = dt.datetime.now()

        self.file_check('Rabochie_listy')

    def get_sales(self, division=None):
        self.auth()
        # Ожидание загрузки страницы и появления элемента
        wait = WebDriverWait(self.driver, 30)
        menu_item = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, 'Отчеты'))
        )
        # Выбор нужного отчета
        print('Выбираю в меню нужный отчет (Отчет по продаже ТС)')
        actions = ActionChains(self.driver)
        # Перемещение курсора к указанному элементу
        actions.move_to_element(menu_item).perform()
        # menu_item = self.driver.find_element(By.LINK_TEXT, 'Рабочие лиcты')
        menu_item = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, 'Отчет по продаже ТС'))
        )
        print("Адрес ссылки:", menu_item.get_attribute("href"))
        menu_item.click()

        # Если выгружаем заявки по BUS, то необходимо выбрать производителя
        wait = WebDriverWait(self.driver, 30)
        if division:
            try:
                menu_item = wait.until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        '//*[@id="interval_type"]'          ### Исправить
                    ))
                )
                # Выбор элемента из выпадающего списка
                select = Select(menu_item)
                select.select_by_visible_text(division)
            except:
                raise Exception(
                    'Элемент для выбора производителя не найден. Возможно, данный элемент недоступен для данного аккаунта.'
                )           

        print('Выставляю тип выгрузки за месяц')
        menu_item = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="interval_type"]'
            ))
        )
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_visible_text("МС")
        time.sleep(1)

        print('Выставляю год начала периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="start_year"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.start_date.year))
        time.sleep(1)

        print('Выставляю месяц начала периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="counter_min"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.start_date.month))
        time.sleep(1)

        print('Выставляю год конца периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="end_year"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.end_date.year))
        time.sleep(1)

        print('Выставляю месяц конца периода')
        menu_item = self.driver.find_element(By.XPATH, '//*[@id="counter_max"]')
        # Выбор элемента из выпадающего списка
        select = Select(menu_item)
        select.select_by_value(str(self.end_date.month))
        time.sleep(1)

        wait = WebDriverWait(self.driver, 10)
        print('Нажимаю ОБНОВИТЬ ДАННЫЕ')
        menu_item = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="grand_selector"]/div[1]/div/table[2]/tbody/tr/td[1]/div/div/div[2]/button'
            ))
        )
        menu_item.click()
    
        wait = WebDriverWait(self.driver, 60)
        print('Добавляю поля в выгрузку')
        # Ожидание загрузки страницы и появления элемента шестеренки
        element = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                '//*[@id="event-grid"]/div[1]/div[1]/button/i'
            ))
        )
        element.click()
        print('Шестеренку нажал')

        # Добавление полей 
        for _ in range(1,25):
            try:
                wait = WebDriverWait(self.driver, 3)
                field_item = wait.until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        f'//*[@id="modal_customizable"]/div/div/div[2]/div/form/fieldset/div/div[1]/select/option[1]'
                    ))
                )
                field_item.click()
                button = self.driver.find_element(
                    By.XPATH,
                    '//*[@id="modal_customizable"]/div/div/div[2]/div/form/fieldset/div/div[2]/a[1]'
                )
                button.click()
                print('Поле {_} готово.')
            except:
                break       
        
        ok_button = self.driver.find_element(
            By.XPATH,
            '//*[@id="modal_customizable"]/div/div/div[3]/button'
        )
        ok_button.click()
        time.sleep(1)        


        # Перед скачиванием файла экселя очищаем целевую папку
        file_pattern = os.path.join(self.path, 'Otchet_po_prodazhe*')
        matching_files = glob.glob(file_pattern)
        if matching_files:
            for file_path in matching_files:
                os.remove(file_path)
                print(f"Удален файл: {file_path}")
        else:
            print(f"Файлы, соответствующие шаблону имени 'Otchet_po_prodazhe*', не найдены.")

        # Скачивание отчета в эксель
        print('Нажимаю кнопку.')
        time.sleep(10) 
        wait = WebDriverWait(self.driver, 10)
        menu_item = wait.until(
                    EC.element_to_be_clickable((
                        By.XPATH,
                        '//*[@id="grand_selector"]/div[1]/div/table[2]/tbody/tr/td[6]/div/a'
                    ))
                )

        self.driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', inline: 'center'});",
            menu_item
        )
        time.sleep(1) 
        actions.move_to_element(menu_item).click().perform()
        self.ts = dt.datetime.now()

        self.file_check('Otchet_po_prodazhe')

    def file_check(self, data_type):

        counter = 0
        file_pattern = os.path.join(self.path, fr'{data_type}_{self.ts.year}_{self.ts.month}_*.xlsx')
        print('Ищу следующий файл:', file_pattern)

        while True:
            if counter > 36:
                raise Exception('За 3 мины выгрузки не произошло!')
            matching_files = glob.glob(file_pattern)
            if matching_files:
                print(f"Найдены файлы, соответствующие части имени:")
                for file_path in matching_files:
                    print(file_path)
                break
            else:
                print(f"Файлы, соответствующие части имени, не найдены.")
                time.sleep(5)
                counter += 1
        self.driver.quit()
        self.file = matching_files[0]
        self.file_pattern = f'{data_type}*'
