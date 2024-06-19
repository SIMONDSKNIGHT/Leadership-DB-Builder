from arelle import Cntlr
from arelle.ModelManager import ModelManager
from arelle.FileSource import openFileSource

# Initialize Arelle controller
controller = Cntlr.Cntlr()
modelManager = ModelManager(controller)

# Path to your XBRL file
xbrlFilePath = 'files/XBRL/AuditDoc/jpaud-aai-cc-001_E21342-000_2023-03-31_01_2023-06-29.xbrl'

# Load the XBRL file
modelXbrl = modelManager.load(openFileSource(xbrlFilePath, controller), 'Loading XBRL file')

# Print out the data in the file
for fact in modelXbrl.facts:
    print(f"Context ID: {fact.contextID}, Concept: {fact.qname}, Value: {fact.value}")

modelXbrl.close()