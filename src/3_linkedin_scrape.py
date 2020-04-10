from linkedin_scraper import (
    actions,
    Person
)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


options = Options()
# options.binary_location = '/usr/bin/google-chrome-stable'
options.binary_location = '/usr/bin/brave-browser-stable'
driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver',
                          options=options)

email = ""
password = ""
actions.login(driver, email, password)
person = Person(
    "https://www.linkedin.com/in/andre-iguodala-65b48ab5", driver=driver)

# person.scrape(close_on_complete=False)

print(person)
