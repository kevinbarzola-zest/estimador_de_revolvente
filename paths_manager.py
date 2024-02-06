import json
import tkinter
from functools import partial
from tkinter import filedialog

# number of paths that were not originally in the storage, but have been retrieved with this code
paths_retrieved = 0

def update_labels(b1, l1, l2, l3, missing_paths_l, root):
    """
    Changes the text of the window to show the user that his input has been taken.
    If there are any missing paths left, it triggers the function to prompt the user to manually select the next missing path.

    Args:
        b1 (tkinter.Button): Object representing a tkinter button in our main window
        l1 (tkinter.Label): Object representing a tkinter label in our main window
        l2 (tkinter.Label): Object representing a tkinter label in our main window
        l3 (tkinter.Label): Object representing a tkinter label in our main window
        missing_paths_l (list): List version of missing_paths, which is a dictionary with keys for the missing paths
            and values that are lists with the shape [name, suggested path, path type]
        root (tkinter.Tk): Object representing a tkinter window (our main window)

    Returns: None
    """
    global paths_retrieved
    if paths_retrieved == len(missing_paths_l):
        root.destroy()
        return
    l1.config(text=f"Seleccionar el archivo de {missing_paths_l[paths_retrieved][1][0]}")
    print(f"Seleccionar el archivo de {missing_paths_l[paths_retrieved][1][0]}")
    l2.config(text=f"Ruta Sugerida: {missing_paths_l[paths_retrieved][1][1]}")
    print(f"Ruta Sugerida: {missing_paths_l[paths_retrieved][1][1]}")
    l3.config(text="")
    b1.config(command=partial(load_path_input_window, l3, missing_paths_l[paths_retrieved][1][2], root))


def save_path(b1, l1, l2, l3, data, missing_paths_l, root):
    """
    Saves the directory manually selected by the user in the storage (json file)

    Args:
        b1 (tkinter.Button): Object representing a tkinter button in our main window
        l1 (tkinter.Label): Object representing a tkinter label in our main window
        l2 (tkinter.Label): Object representing a tkinter label in our main window
        l3 (tkinter.Label): Object representing a tkinter label in our main window
        missing_paths_l (list): List version of missing_paths, which is a dictionary with keys for the missing paths
            and values that are lists with the shape [name, suggested path, path type]
        root (tkinter.Tk): Object representing a tkinter window (our main window)

    Returns: None
    """
    global paths_retrieved

    data[missing_paths_l[paths_retrieved][0]] = l3.cget("text")[14:]
    with open('C:/Users/Public/paths.json', 'w') as outfile:
        json.dump(data, outfile)
    paths_retrieved += 1
    update_labels(b1, l1, l2, l3, missing_paths_l, root)


def load_path_input_window(l3, last_type, root):
    """
    Loads the appropriate window for manually selecting the path for the directory or the file

    Args:
        l3 (tkinter.Label): Object representing a tkinter label in our main window
        root (tkinter.Tk): Object representing a tkinter window (our main window)

    Returns: None
    """
    if last_type == "DIR":
        path_loaded = filedialog.askdirectory(parent=root, initialdir="/", title='SELECCIONA CARPETA')
    else:
        path_loaded = filedialog.askopenfilename(parent=root, initialdir="/", title='SELECCIONA ARCHIVO')
    l3.config(text="Seleccionado: " + path_loaded)

def get_paths_from_storage():
    """
    Gets the saved paths (a serialized dict) from a json file

    Returns:
        dict: a dict of saved paths for the keys ["SCRAPPER.CNOTAS", "SCRAPPER.BDPRODUCTOS"]
    """
    with open('C:/Users/Public/paths.json', 'r+') as json_of_paths:
        try:
            data = json.load(json_of_paths)
        except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
            data = {}
            json_of_paths.write(json.dumps(data))
    return data


def show_main_input_window(data, missing_paths):
    """
    Builds and displays a window for manually selecting the missing paths

    Args:
        data (dict): dictionary of saved paths for the keys ["SCRAPPER.CNOTAS", "SCRAPPER.BDPRODUCTOS"]
        missing_paths (dict): dictionary with keys for the missing paths
            and values that are lists with the shape [name, suggested path, path type]

    Returns:
        dict: a dict of saved paths for the keys ["SCRAPPER.CNOTAS", "SCRAPPER.BDPRODUCTOS"]
    """
    missing_paths_l = list(missing_paths.items())
    global paths_retrieved

    root = tkinter.Tk()
    root.protocol("WM_DELETE_WINDOW", root.quit)
    root.geometry("700x500")
    root.configure(background='#686969')

    l1 = tkinter.Label(root, text=f"Seleccionar el archivo de {missing_paths_l[paths_retrieved][1][0]}", background='#686969')
    l1.config(font=("Courier", 14))
    l2 = tkinter.Label(root, text=f"Ruta Sugerida: {missing_paths_l[paths_retrieved][1][1]}", background='#686969')
    l2.config(font=("Courier", 10))
    l3 = tkinter.Label(root, text="", wraplength=500, background='#686969')
    l3.config(font=("Courier", 10))

    b1 = tkinter.Button(root, text="Elegir archivo",
                        command=partial(load_path_input_window, l3, missing_paths_l[paths_retrieved][1][2], root))
    b2 = tkinter.Button(root, text="Confirmar",
                        command=partial(save_path, b1, l1, l2, l3, data, missing_paths_l, root)
                        )
    l1.pack(pady=20)
    l2.pack()
    l3.pack()
    b1.pack()
    b2.pack()
    tkinter.mainloop()

    return data


def get_paths(paths_info):
    """
    Checks the storage for a list of paths stored in a json file

    Returns:
        dict: dictionary of saved paths for the keys ["SCRAPPER.CNOTAS", "SCRAPPER.BDPRODUCTOS"]
    """

    missing_paths = {}
    data = get_paths_from_storage()
    for key in paths_info.keys():
        if key not in data.keys():
            print("No encontrado: ", key)
            missing_paths[key] = [paths_info[key][0], paths_info[key][1], paths_info[key][2]]

    if missing_paths:
        data = show_main_input_window(data, missing_paths)
    return data
