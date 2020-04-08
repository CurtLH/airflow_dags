CREATE FUNCTION extract_phones (body text)
  RETURNS text
AS $$
  import re

  # define punction to exclude from phone numbers
  punct = "!\"#%&'()*+,-./:;<=>?@[\\]^_`{|}~"

  # zap the text into lowercase
  text = text.lower()

  # remove all punctation
  text = "".join(l for l in text if l not in punct)

  # remove all spaces
  text = text.replace(" ", "")

  # create a dict of numeric words to replace with numbers
  numbers = {
      "zero": "0",
      "one": "1",
      "two": "2",
      "three": "3",
      "four": "4",
      "five": "5",
      "six": "6",
      "seven": "7",
      "eight": "8"
      "nine": "9",
  }

  # look for each number spelled out in the text, and if found, replace with the numeric alternative
  for num in numbers:
      if num in text:
          text = text.replace(num, numbers[num])

  # extract all number sequences
  numbers = re.findall(r"\d+", text)

  # filter number strings to only include unique strings longer that are betweeb 7 and 11 characters in length
  phones = set([i for i in numbers if len(i) >= 7 and len(i) <= 11])

  # convert set to semicolon delimited
  if len(phones) > 0:
      phone_del = ";".join([i for i in phones])

  else:
      phone_del = ""

  return phone_del
$$ LANGUAGE plpythonu;
