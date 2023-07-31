class webScrapper:
    def __init__(self):
        self.__urlMusi = "https://www.musimundo.com/telefonia/telefonos-celulares/c/82/samsung?q=%3Arelevance%3Abrand%3Amarca_SAMSUNG%3ApriceRn%3A%2524100.000%2B-%2B%2524499.999%252C99"
        self.__titleMusi = '//div[@class="mus-pro-desc"]/h3[@class="mus-pro-name"]/a/text()'
        self.__priceMusi = '//div[@class="mus-pro-price-box"]//span[@class="mus-pro-price-number "]/span/text()'
        self.__urlMovi = 'https://tienda.movistar.com.ar/?manufacturer=49'
        self.__titleMovi = '//a/span[@class="name"]/text()'
        self.__priceMovi = '//div[@class="price-nowrap"]//span[@class="price"]/text()'
        self.__urlPer = "https://tienda.personal.com.ar/celulares/samsung?filter_interaction=true&taxIncludeAmount=100000a499999"
        self.__titlePer = '//div[@class="content"]/h3[@class="ui header product-name"]/text()'
        self.__pricePer = '//div[@class="description"]/div/p/text()'
        self.__urlDolar = 'https://www.cronista.com/MercadosOnline/dolar.html'

    def dataMusi(self):
        data = {"urlMusi" : self.__urlMusi ,
                "titleMusi" : self.__titleMusi,
                "priceMusi" : self.__priceMusi}
        return data

    def dataMovi(self):
            data = {"urlMovi" : self.__urlMovi ,
                    "titleMovi" : self.__titleMovi,
                    "priceMovi" : self.__priceMovi}
            return data        

    def dataPer(self):
            data = {"urlPer" : self.__urlPer ,
                    "titlePer" : self.__titlePer,
                    "pricePer" : self.__pricePer}
            return data

    def dataDolar(self):
            data = self.__urlDolar
            return data    