import dl

def updateFlow(request):
    data = dl.Covid19Data() 
    data.getData()
    data.processData()


updateFlow(None)
