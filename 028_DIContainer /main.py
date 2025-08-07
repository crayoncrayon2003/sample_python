from containers import Container

def main():
    print("--- Case 1: Using MyServiceEmail ---")

    emailContainer = Container()
    emailContainer.config.mytype.from_value("email")    # mytype = email

    emailService = emailContainer.myServiceProvider()   # Instance is ProviderMyServiceEmail
    emailService.register_user("Alice")                 # Instance is ProviderMyServiceEmail

    print("-" * 40)

    # --- ケース2: SMSで通知を送る ---
    print("--- Case 2: Using MyServiceSMS ---")

    smsContainer = Container()
    smsContainer.config.mytype.from_value("sms")        # mytype = email
    smsService = smsContainer.myServiceProvider()       # Instance is ProviderMyServiceSMS
    smsService.register_user("Bob")

    print("-" * 40)

if __name__ == "__main__":
    main()