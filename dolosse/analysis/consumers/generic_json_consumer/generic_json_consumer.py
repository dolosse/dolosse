"""Reads json data from a kafka topic and graphs it"""

from argparse import ArgumentParser
from curses import initscr, cbreak, nocbreak, endwin
from curses import ERR as NoInput
from json import loads, decoder

from confluent_kafka import Consumer
from matplotlib import pyplot


def find_number(value):
    """Convert value to a number, if posible, and indicate success or failure
    Parameters:
        value: The variable to convert (could be _anything_)
    Return values: First the converted number, as float or int; then
    a boolean that shows whether the conversion succeeded (true) or failed
    (false)
    """
    number = 0 #Default value
    valid = True #Also a default (one of these will usually change)
    try:
        number = float(value)
    except (ValueError, TypeError):
        valid = False
        if not valid:
            try:
                number = int(value, 16)  # Catch hexadecimals
                valid = True
            except (ValueError, TypeError):
                valid = False
    return number, valid


def list_append_value(items, name, new_val, validity):
    """Add a variable to he end of the list if it is valid. Otherwise,
    add a copy of the last value; or create a new list containing only
    the value zero if there is no last value.
    Parameters:
        items: List of variables. A dict containing only lists.
        name: Name of the variable to modify.
        new_val: New value to use (only if valid)
        validity: Whether or not the variable is, in fact, valid
    Return: Nothing direct, but it modifies list as necessary
    """
    if name in items:
        if validity:
            items[name].append(new_val)
        else:
            items[name].append(
                items[name][-1])
    else:
        if validity:
            items[name] = [new_val]
        else:
            items[name] = [0]


def populate(variables, decode, backfill=False, frontfill=False, prepend=""):
    """Takes the data from the dict and populates the variables
    Parameters:
        variables: Python dict, either empty or containing a number of variables
                   organised with variable name as the key and a list of values
                   over time as the value. Modified by this function.
        decode: Dictionary containing variable data at the new time interval,
                as decoded directly from the JSON message and updated from the
                running variable intervals (will only update variables in this
                dictionary)
        prepend: String with which to prepend all variable names. Only used when
                 recursing; let this take the default value when calling this function
                 from anywhere outside this function. If that is somehow impossible,
                 then use the same value every time.
    Return value: No direct return; this function rather populates the 'variables' dict
                  by appending new data from the 'decode' dict.
    """
    if isinstance(decode, dict):
        for other_key in decode.keys():
            valid = True
            name = prepend + other_key
            following, valid = find_number(decode[other_key])
            if isinstance(decode[other_key], dict):
                populate(variables,
                         decode[other_key],
                         False,
                         backfill or frontfill,
                         name + ": ")
            elif isinstance(decode[other_key], list):
                populate(variables,
                         decode[other_key],
                         False,
                         backfill or frontfill,
                         name)
            else:
                list_append_value(variables, name, following, valid)
    elif isinstance(decode, list):
        for counter in range(len(decode)):
            populate(variables,
                     decode[counter],
                     False,
                     backfill or frontfill,
                     prepend + "[" + str(counter) + "]" + ": ")
    else:
        value, valid = find_number(decode)
        list_append_value(variables, prepend, value, valid)


def get_args():
    """Creates and returns a kafka consumer
    Return value: A dict containing the topic name, group name, and server name
                  suitable for passing as a parameter to the graph function.
    """
    parser = ArgumentParser(description=
                            "A generic consumer that graphs any JSON data that it consumes")
    parser.add_argument("--topic", default="default",
                        help="The name of the kafka topic from which to read")
    parser.add_argument("--group", default="my-group",
                        help="The kafka group in which the topic exists")
    parser.add_argument("--server", default="localhost:9092",
                        help="Name and port of the kafka server (e.g. localhost:9092)")

    # To consume latest messages and auto-commit offsets
    return parser.parse_args()


def redraw(graphs):
    """Redraw all figures from a dict of figures. Call often.
    Parameters:
        figures: A dict containing a list of figures to redraw.
    Return value: None. Merely redraws the figures in the list.
    """
    for key in graphs.keys():
        graphs[key]["figure"].canvas.draw()
        graphs[key]["figure"].canvas.flush_events()


def update_data(value):
    """Returns the list of data, given some json input
    Parameters:
    value: A string containing the latest JSON input received.
    Return value: The string converted to a dictionary if possible, or a blank dictionary
    """
    try:
        update = loads(value)
    except decoder.JSONDecodeError as problem:
        print("Error: Flawed JSON in value: ", value)
        print("Error message: ", problem)
        update = {}
    return update


def graph(arguments):
    """Main function - reads json data from a kafka topic and graphs it
    Parameters:
    arguments: Command-line arguments as a dict. Contains the following:
        topic: String containing the name of the kafka topic from which to read data
        group: String containing the group id to use when reading from kafka
        server: String containing the address of the kafka server
    Return value: None; this function plots the data provided by the named kafka topic,
                  as read by the named group from the named server, and plots it to screen
                  so that the user can see it. It ends when given a keypress.
    """

    screen = initscr()
    cbreak()
    screen.nodelay(True)
    screen.getch()

    print("Generic Graphing Consumer")
    print("Press any key to exit...")

    data = {}
    curve = {}
    graphs = {}
    count = 0
    pyplot.ion()  # Interactive mode on
    consumer = Consumer({"bootstrap.servers": arguments.server,
                         "group.id": arguments.group,
                         "default.topic.config": {
                             "auto.offset.reset": "earliest"},
                         "enable.auto.commit": False})
    consumer.subscribe([arguments.topic])
    keyboard_input = NoInput

    while True:
        if keyboard_input != NoInput:
            break
        keyboard_input = screen.getch()
        message = consumer.consume(num_messages=1, timeout=0.2)
        if (message is None) or (message == []):
            redraw(graphs)
        elif message[0].error():
            print("Something went wrong with the read!")
            print(message.error())
        else:
            data = update_data(message[0].value())
            count += 1

            populate(curve, data, True)

            for key in curve.keys():
                if key in graphs.keys():
                    graphs[key]["axis"][0].set_ydata(curve[key])
                    graphs[key]["axis"][0].set_xdata(list(range(len(curve[key]))))
                    graphs[key]["plot"].relim()
                    graphs[key]["plot"].autoscale_view()
                else:
                    graphs[key] = {}
                    graphs[key]["figure"], graphs[key]["plot"] = pyplot.subplots()
                    graphs[key]["axis"] = graphs[key]["plot"].plot(curve[key])
                    graphs[key]["plot"].set_autoscaley_on(True)
                    graphs[key]["plot"].set_autoscalex_on(True)
                    graphs[key]["plot"].set_title(key)

            redraw(graphs)

    nocbreak()
    endwin()


if __name__ == '__main__':
    graph(get_args())
