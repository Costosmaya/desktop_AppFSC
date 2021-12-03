from PyQt5 import QtWidgets, uic
from PyQt5 import QtCore
from PyQt5.QtCore import QObject, QThread, pyqtSignal, QMutex
import sys

from UI.design import Ui_MainWindow

from UI.dialog import Ui_Dialog

from movimientos_FSC import movimientos

mutex = QMutex()

class Worker(QObject):
    finished = pyqtSignal()
    progress = pyqtSignal(int)
    message = pyqtSignal(str)

    def Movimientos(self,args, path):
        mutex.lock()
        self.progress.emit(25)
        movimientos(self,args, path)
        mutex.unlock()
        self.finished.emit()

class Dialog(QtWidgets.QDialog):
    signal = pyqtSignal()

    def __init__(self, parent= None):
        super(Dialog,self).__init__(parent)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self)

        self.ui.dialogBtn.clicked.connect(self.close)

    def show_message(self, message):
        self.ui.label.setText(message)
        super(Dialog,self).exec()
        self.signal.emit()

class mainWindow(QtWidgets.QMainWindow):
    signal = pyqtSignal()
    message = ""
    def __init__(self):
        super(mainWindow,self).__init__()

        self.ui = Ui_MainWindow()

        self.ui.setupUi(self)

        self.ui.dirButton.clicked.connect(self.pathChooser)
        self.ui.getButton.clicked.connect(self.StartThread)
        self.dialog = Dialog()
        self.dialog_done = False
    
    def pathChooser(self):
        directory = ""
        directory = str(QtWidgets.QFileDialog.getExistingDirectory(self, "Seleccione la carpeta"))
        self.ui.rutaLabel.setText(self.ui.rutaLabel.text().split(':')[0]+':'+directory)
    
    def generateArgs(self):
        choiceF = self.ui.fechaFButton.isChecked()
        choiceOp = self.ui.opFButton.isChecked()
        args = ""
        if choiceF:
            choiceRange = self.ui.dateChoser.isChecked()
            args = ['\''+str(self.ui.sdateEdit.date().toPyDate())+'\'', '\''+str(self.ui.edateEdit.date().toPyDate())+'\''] if choiceRange else ['\''+str(self.ui.sdateEdit.date().toPyDate())+'\'']

            return args;
        elif choiceOp:
            args = self.ui.lineEdit.text()
            self.ui.lineEdit.setText("")
            return args;
        else:
            return;
        
    def createThread(self,args, path):
        thread = QThread(parent=self)
        worker = Worker()
        worker.moveToThread(thread)
        

        thread.started.connect(lambda: worker.Movimientos(args, path))
        worker.finished.connect(thread.quit)
        worker.progress.connect(self.setProgressVal)
        worker.message.connect(self.setMessage)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        self.ui.progressBar.setEnabled(True)
        self.ui.getButton.setEnabled(False)
        thread.finished.connect(lambda: self.ui.getButton.setEnabled(True))

        thread.finished.connect(lambda: self.ui.progressBar.setValue(100))

        thread.finished.connect(lambda: self.ui.progressBar.setEnabled(False))


        thread.finished.connect(lambda: self.show_dialog(self.message))
        return thread

    def setProgressVal(self,val):
        self.ui.progressBar.setValue(val)

    def setMessage(self, val):
        self.message = val

    def complete_dialog(self):
        self.dialog_done = True    
    
    def wait_for_dialog(self):
        while not self.dialog_done:
            pass
        self.dialog_done = False
        self.ui.progressBar.setValue(0)
        

    def StartThread(self):
        args = self.generateArgs()
        
        parts = self.ui.rutaLabel.text().split(':')
        path = self.ui.rutaLabel.text().split(':')[1].strip()+":"+parts[2] if len(parts)> 2 else parts[1].strip()

        thread = self.createThread(args, path)

        thread.start()
    
    def show_dialog(self, message):
        self.message = message
        self.signal.emit()

        self.wait_for_dialog()

        

    
    
        

app = QtWidgets.QApplication([])

application = mainWindow()

application.show()

dialog = Dialog()

application.signal.connect(lambda: dialog.show_message(application.message))

dialog.signal.connect(application.complete_dialog)



sys.exit(app.exec())