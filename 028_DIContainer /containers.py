from dependency_injector import containers, providers
import services

class Container(containers.DeclarativeContainer):
    # コンフィグ
    config = providers.Configuration()

    # 1. コンテナ内で常に同じインスタンスを返すプロバイダー定義
    ProviderMyServiceEmail = providers.Singleton(services.MyServiceEmail)
    ProviderMyServiceSMS   = providers.Singleton(services.MyServiceSMS)

    # 2. 設定値に応じてサービスを切替えるセレクター
    # 'email' なら ProviderMyServiceEmail  を返す
    # 'sms'   なら ProviderMyServiceSMS    を返す
    SelectorMyService = providers.Selector(
        config.mytype,
        email = ProviderMyServiceEmail,
        sms   = ProviderMyServiceSMS,
    )

    # 3. UserServiceのプロバイダー定義
    # Factory: 呼び出されるたびに新しいインスタンスを作成する
    # UserServiceのコンストラクタ引数(notification_service)に、
    # 上で定義した notification_service_provider を注入するよう指定
    myServiceProvider = providers.Factory(
        services.UserService,
        myService=SelectorMyService,
    )