from selenium import webdriver
import time

driver = webdriver.Chrome()
driver.get("https://smartup.online")

time.sleep(10)  # Ko‘rishga va tekshirishga vaqt beradi

print("Current page source:")
print(driver.page_source[:1000])  # Faqat 1000 ta belgini chiqaramiz

time.sleep(100)  # Qo‘lda brauzerni tekshirish uchun vaqt
driver.quit()
