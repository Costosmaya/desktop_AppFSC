# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'design.ui'
#
# Created by: PyQt5 UI code generator 5.15.4
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.setWindowModality(QtCore.Qt.NonModal)
        MainWindow.resize(688, 441)
        MainWindow.setAutoFillBackground(False)
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.horizontalLayoutWidget = QtWidgets.QWidget(self.centralwidget)
        self.horizontalLayoutWidget.setGeometry(QtCore.QRect(10, 0, 671, 41))
        self.horizontalLayoutWidget.setObjectName("horizontalLayoutWidget")
        self.horizontalLayout = QtWidgets.QHBoxLayout(self.horizontalLayoutWidget)
        self.horizontalLayout.setContentsMargins(0, 0, 0, 0)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.groupBox1 = QtWidgets.QGroupBox(self.horizontalLayoutWidget)
        self.groupBox1.setFlat(False)
        self.groupBox1.setCheckable(False)
        self.groupBox1.setObjectName("groupBox1")
        self.fechaFButton = QtWidgets.QRadioButton(self.groupBox1)
        self.fechaFButton.setGeometry(QtCore.QRect(40, 10, 111, 21))
        self.fechaFButton.setChecked(False)
        self.fechaFButton.setObjectName("fechaFButton")
        self.opFButton = QtWidgets.QRadioButton(self.groupBox1)
        self.opFButton.setGeometry(QtCore.QRect(290, 10, 101, 21))
        self.opFButton.setObjectName("opFButton")
        self.fechaIButton = QtWidgets.QRadioButton(self.groupBox1)
        self.fechaIButton.setGeometry(QtCore.QRect(170, 10, 91, 21))
        self.fechaIButton.setObjectName("fechaIButton")
        self.horizontalLayout.addWidget(self.groupBox1)
        self.verticalLayoutWidget = QtWidgets.QWidget(self.centralwidget)
        self.verticalLayoutWidget.setGeometry(QtCore.QRect(10, 40, 671, 291))
        self.verticalLayoutWidget.setObjectName("verticalLayoutWidget")
        self.verticalLayout = QtWidgets.QVBoxLayout(self.verticalLayoutWidget)
        self.verticalLayout.setContentsMargins(0, 0, 0, 0)
        self.verticalLayout.setObjectName("verticalLayout")
        self.dateGroup = QtWidgets.QGroupBox(self.verticalLayoutWidget)
        self.dateGroup.setEnabled(False)
        self.dateGroup.setObjectName("dateGroup")
        self.sdateEdit = QtWidgets.QDateEdit(self.dateGroup)
        self.sdateEdit.setGeometry(QtCore.QRect(140, 50, 151, 22))
        self.sdateEdit.setButtonSymbols(QtWidgets.QAbstractSpinBox.PlusMinus)
        self.sdateEdit.setDateTime(QtCore.QDateTime(QtCore.QDate(2021, 1, 1), QtCore.QTime(0, 0, 0)))
        self.sdateEdit.setCalendarPopup(True)
        self.sdateEdit.setObjectName("sdateEdit")
        self.dateChoser = QtWidgets.QRadioButton(self.dateGroup)
        self.dateChoser.setGeometry(QtCore.QRect(20, 20, 91, 17))
        self.dateChoser.setObjectName("dateChoser")
        self.edateEdit = QtWidgets.QDateEdit(self.dateGroup)
        self.edateEdit.setEnabled(False)
        self.edateEdit.setGeometry(QtCore.QRect(430, 50, 151, 22))
        self.edateEdit.setButtonSymbols(QtWidgets.QAbstractSpinBox.PlusMinus)
        self.edateEdit.setDateTime(QtCore.QDateTime(QtCore.QDate(2021, 1, 1), QtCore.QTime(0, 0, 0)))
        self.edateEdit.setCalendarPopup(True)
        self.edateEdit.setObjectName("edateEdit")
        self.fiLabel = QtWidgets.QLabel(self.dateGroup)
        self.fiLabel.setGeometry(QtCore.QRect(60, 50, 71, 16))
        self.fiLabel.setObjectName("fiLabel")
        self.feLabel = QtWidgets.QLabel(self.dateGroup)
        self.feLabel.setGeometry(QtCore.QRect(350, 50, 71, 16))
        self.feLabel.setObjectName("feLabel")
        self.verticalLayout.addWidget(self.dateGroup)
        self.opGroup = QtWidgets.QGroupBox(self.verticalLayoutWidget)
        self.opGroup.setEnabled(False)
        self.opGroup.setObjectName("opGroup")
        self.opLabel = QtWidgets.QLabel(self.opGroup)
        self.opLabel.setGeometry(QtCore.QRect(20, 20, 331, 16))
        self.opLabel.setObjectName("opLabel")
        self.lineEdit = QtWidgets.QLineEdit(self.opGroup)
        self.lineEdit.setGeometry(QtCore.QRect(170, 50, 321, 20))
        self.lineEdit.setObjectName("lineEdit")
        self.verticalLayout.addWidget(self.opGroup)
        self.fileGroup = QtWidgets.QGroupBox(self.verticalLayoutWidget)
        self.fileGroup.setObjectName("fileGroup")
        self.dirButton = QtWidgets.QPushButton(self.fileGroup)
        self.dirButton.setGeometry(QtCore.QRect(50, 30, 131, 41))
        self.dirButton.setObjectName("dirButton")
        self.rutaLabel = QtWidgets.QLabel(self.fileGroup)
        self.rutaLabel.setGeometry(QtCore.QRect(190, 40, 591, 21))
        self.rutaLabel.setObjectName("rutaLabel")
        self.verticalLayout.addWidget(self.fileGroup)
        self.getButton = QtWidgets.QPushButton(self.centralwidget)
        self.getButton.setGeometry(QtCore.QRect(270, 340, 111, 23))
        self.getButton.setObjectName("getButton")
        self.progressBar = QtWidgets.QProgressBar(self.centralwidget)
        self.progressBar.setEnabled(False)
        self.progressBar.setGeometry(QtCore.QRect(10, 370, 671, 23))
        self.progressBar.setProperty("value", 0)
        self.progressBar.setTextVisible(True)
        self.progressBar.setOrientation(QtCore.Qt.Horizontal)
        self.progressBar.setInvertedAppearance(False)
        self.progressBar.setObjectName("progressBar")
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 688, 21))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        self.fechaFButton.toggled['bool'].connect(self.dateGroup.setEnabled)
        self.fechaFButton.toggled['bool'].connect(self.opGroup.setDisabled)
        self.dateChoser.toggled['bool'].connect(self.edateEdit.setEnabled)
        self.opFButton.toggled['bool'].connect(self.opGroup.setEnabled)
        self.fechaIButton.toggled['bool'].connect(self.dateGroup.setEnabled)
        self.fechaIButton.toggled['bool'].connect(self.opGroup.setDisabled)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Control COC FSC Mayaprin"))
        self.groupBox1.setTitle(_translate("MainWindow", "Filtros"))
        self.fechaFButton.setText(_translate("MainWindow", "Fecha Facturación"))
        self.opFButton.setText(_translate("MainWindow", "No. Orden(es)"))
        self.fechaIButton.setText(_translate("MainWindow", "Fecha Ingreso"))
        self.dateGroup.setTitle(_translate("MainWindow", "Fecha"))
        self.dateChoser.setText(_translate("MainWindow", "Rango fechas"))
        self.fiLabel.setText(_translate("MainWindow", "Fecha Inicial:"))
        self.feLabel.setText(_translate("MainWindow", "Fecha Final:"))
        self.opGroup.setTitle(_translate("MainWindow", "No Orden(es)"))
        self.opLabel.setText(_translate("MainWindow", "Escriba todas los números de órdenes, separados por una coma (,)"))
        self.fileGroup.setTitle(_translate("MainWindow", "Ruta Almacenamiento de Archivo"))
        self.dirButton.setText(_translate("MainWindow", "seleccionar"))
        self.rutaLabel.setText(_translate("MainWindow", "ruta:"))
        self.getButton.setText(_translate("MainWindow", "Obtener Informe"))
