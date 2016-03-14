Enigma-yarn-app
===============
yarn jar enigma-yarn-app-1.1.0.jar -jar /axp/gcp/cpsetlh/dev/enigma-yarn-app-1.1.0.jar -appMasterClass  com.tito.sampleapp.enigma.EnigmaAppMaster -enigmaCount 3 -enigmaTempDir /axp/gcp/cpsetlh/dev/test/enigma/enigmaTempDir -plainTextPath  /axp/gcp/cpsetlh/dev/test/enigma/plain/plain.txt -cipherTextPath /axp/gcp/cpsetlh/dev/test/enigma/cipher/cipher.text -keyPath /axp/gcp/cpsetlh/dev/test/enigma/EnigmaKey.key  -operation e


yarn jar enigma-yarn-app-1.1.0.jar -jar /axp/gcp/cpsetlh/dev/enigma-yarn-app-1.1.0.jar -appMasterClass  com.tito.sampleapp.enigma.EnigmaAppMaster -enigmaTempDir /axp/gcp/cpsetlh/dev/test/enigma/enigmaTempDir -plainTextPath  /axp/gcp/cpsetlh/dev/test/enigma/plain/plain_decrypted.txt -cipherTextPath /axp/gcp/cpsetlh/dev/test/enigma/cipher/cipher.text  -keyPath /axp/gcp/cpsetlh/dev/test/enigma/EnigmaKey.key -operation d