from PyQt5 import QtWidgets, uic
import sys

sys.path.insert(0,'./desktop_App FSC/UI')

from design import Ui_MainWindow

from movimientos_FSC import movimientos

class mainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(mainWindow,self).__init__()

        self.ui = Ui_MainWindow()

        self.ui.setupUi(self)

        self.ui.dirButton.clicked.connect(self.pathChooser)
        self.ui.getButton.clicked.connect(self.generateQuery)
    
    def pathChooser(self):
        directory = ""
        while len(directory) == 0:
            directory = str(QtWidgets.QFileDialog.getExistingDirectory(self, "Seleccione la carpeta"))
        self.ui.rutaLabel.setText(self.ui.rutaLabel.text()+directory)
    
    def generateQuery(self):
        choiceF = self.ui.fechaFButton.isChecked()
        choiceOp = self.ui.opFButton.isChecked()
        args = ""
        if choiceF:
            choiceRange = self.ui.dateChoser.isChecked()
            args = [str(self.ui.sdateEdit.date().toPyDate()), str(self.ui.edateEdit.date().toPyDate())] if choiceRange else [str(self.ui.sdateEdit.date().toPyDate())]

            print(args)
        elif choiceOp:
            args = self.ui.lineEdit.text()
            print(args)
            self.ui.lineEdit.setText("")
        else:
            return;
        
        movimientos(args)
    
        

app = QtWidgets.QApplication([])

application = mainWindow()

application.show()

sys.exit(app.exec())