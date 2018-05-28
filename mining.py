import pandas as pd
import sys
import emoji
import re
import itertools
from stop_words import get_stop_words 
from ftfy import fix_text


def main():
    
    # Read inputs from command line
    script   = sys.argv[0]
    language = sys.argv[1]
    language = language.replace('-','')
    filename = sys.argv[2]
    
    # Read file and save to a list of dictionaries
    list_messages = read_file(filename)
    
    # Clean and pre-process content
    processed = preprocess_msg(list_messages)

    # Convert to a dataframe with a row per word
    df_msg_word = convert_to_df(list_messages, processed, language)

    # Save to csv
    df_msg_word.to_csv('whatsapp_chat.csv', sep = '\t')
    
    
    
def read_file(filename):
    ''' Read data file in different formats and 
        save output to a list of dictionaries 
        with sender, datetime and content'''
    
    with open(filename, 'r') as f:
        
        # Look for this text structure
        s = re.findall('\[*(\d+.\d+.\d+\s+\d+:+\d+|\d+.\d+.\d+\s+\d+:+\d+:+\d+)(\]\s|\s-\s)(\w+|\w+\s\w+|\w+\s\w+\s\w+):\s(.*)',                        f.read())
        
        # If format does not match, stop
        if s == []:
            print("Format not recognised")
            sys.exit()
    
    # Create list of dictionaries of messages
    list_msg = []
    for msg in s:
        dict_ = dict({'TimeStamp': msg[0], 'Sender_name': msg[2], 'Content': msg[3]})
        list_msg.append(dict_)  
        
    # return list of dictionaries    
    return(list_msg)



def preprocess_msg(list_msg):
    '''Clean messages by reading emoji, fixing bad unicode characters,
       removing url, most special characters and converting each message
       to a list of words'''
    out1 = []
    out2 = []

    # Iterate over messages
    for message in list_msg: 

        # If there is a content key...
        if 'Content' in message.keys():
            # Look in the content key
            clean_msg = fix_text(message['Content'])

            # Remove URLs
            clean_msg = re.sub(r"http\S+", "", clean_msg)

            # Format multimedia messages
            if '<' in clean_msg and 'omit' in clean_msg:
                clean_msg = clean_msg.replace(' ','_')

            # Convert letters to lower case
            clean_msg = clean_msg.lower()

            # Iterate over characters in the message        
            for char in clean_msg:            
                # Remove special characters
                if not(char.islower() or char in [' ','_',':',"'",'<','>'] or char in emoji.UNICODE_EMOJI): 
                    clean_msg = clean_msg.replace(char, "")

                # If emoji, add a space before the emoji
                if char in emoji.UNICODE_EMOJI:
                    clean_msg  = emoji.demojize(clean_msg.replace(char,' ' + char))

            if "::" in clean_msg: clean_msg = clean_msg.replace('::',': :')

            # Create a list of words
            out1.append(len(clean_msg.split()))
            out2.append(clean_msg.split()) 
            
    return({'number_words':out1, 'clean_list':out2})
    
    
    
def convert_to_df(list_msg, proc, lan):
    
    ''' Convert messages to a pandas dataframe with a row per word, including 
        sender, time, and flags for stop words and emoji '''
    
    # Convert list of dictionaries to dataframe
    df_out = pd.DataFrame(list_msg)

    # Filter rows of messages with content, take only names and time
    content_true = [isinstance(elem, str) for elem in df_out['Content']]
    df_out       = df_out[content_true].reset_index()
    df_out       = df_out[['Sender_name', 'TimeStamp']]

    # Fix bad unicode characters in sender and change time format, 
    # append columns with the number of words per message
    df_out['Sender_name']  = df_out['Sender_name'].apply(fix_text)
    df_out['number_words'] = proc['number_words']

    # Repeat row as many times as words in the the message
    df_out = df_out.set_index(['Sender_name'])['TimeStamp'].repeat(df_out['number_words']).reset_index()
    
    # Append column with flattened list of words, indicator of emoji and stop word    
    all_words       = list(itertools.chain(*proc['clean_list']))
    stop_words_sp   = get_stop_words(lan)
    df_out['word']  = all_words
    df_out['stop']  = [word in stop_words_sp for word in all_words]    
    df_out['emoji'] = [':' in word for word in df_out['word']]
    
    return(df_out)      


main()