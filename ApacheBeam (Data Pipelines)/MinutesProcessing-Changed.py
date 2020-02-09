import apache_beam as beam
from apache_beam.io import fileio, ReadFromText, ReadFromTextWithFilename
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

import datetime
import os
from bs4 import BeautifulSoup
import re


class RunTimeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments
        parser.add_value_provider_argument(
            '--input',
            type=str,
            default='gs://khandeas-fedminutesanalysis/raw/minutes/2019/20191030.html',
            help='Path of the file to read from')

        parser.add_value_provider_argument(
            '--output',
            type=str,
            default='gs://khandeas-fedminutesanalysis/dataflow/processed/processedminutes',
            help='Output file to write results to.')

        parser.add_value_provider_argument(
            '--category',
            type=str,
            default='minutes',
            help='Category of document processing')


class WordExtractingDoFn_Final(beam.DoFn):

    def __init__(self, min_chg_year=2009):    
        beam.DoFn.__init__(self)
        self.min_chg_year = min_chg_year

    def process(self, element):
        """Returns parsed text after parsing by beautiful soup
        Args:
          element: the element being processed
        Returns:
          The processed element.
        """
        from bs4 import BeautifulSoup  # import stmts in the function to facilitate worker access
        import re

        re_headings = re.compile(r'.*(staff|participants.|committee)+.*(economic|financial|policy)+.*\s*.*',
                                 re.IGNORECASE)  # regex for clear topics
        re_start_para = re.compile(r'By unanimous vot.* [\w\n\s]*[.]',
                                   re.IGNORECASE)  # regex for indication of start of actual meat of minutes
        re_end_para = re.compile(r'Vot.* for this \w+', re.IGNORECASE)  # regex for indicating where the minutes stop
        re_year = re.compile(r'[\d]{4}')  # regex for extracting year from file name in element(1)

        reconstructed_html = ''.join(element[1])
        statement = BeautifulSoup(reconstructed_html, 'html.parser')
        headings = statement.findAll('strong')
        words = ''

        # file year. topics only work for minutes after 2008.
        tgt_year = int(re.search(re_year, element[0]).group(0))  # extract year from file name

        # case where there are clear topics within the meeting minutes. this happened from 2011.
        if tgt_year >= self.min_chg_year and len(headings) >= 2:
            for heading in headings:
                result = re.search(re_headings, heading.text)
                if result != None:
                    start = statement.text.find(heading.text) + len(heading.text)
                    target = heading.findNext('strong')
                    if target != None:
                        end = statement.text.find(target.text)
                    else:
                        end = len(statement.text)
                    content = statement.text[start:end]
                    words = words + content

                    # case where there are no topic within minutes. Have to determine where to start and stop
        else:
            results_start = re.finditer(re_start_para, statement.text)
            results_end = re.finditer(re_end_para, statement.text)

            try:
                first, *_ = results_end  
                start_pos = 0  
                end_pos = first.start()
                # check for the first start position that is before the end position
                for x in results_start:
                    if x.end() >= end_pos:
                        break
                    else:
                        start_pos = x.end()
                if (start_pos != 0):
                    content = statement.text[
                              start_pos:end_pos]  
                    words = content
                else:
                    print('file had a 0 start {}'.format(element[0]))
            except:
                # print('file had issues {}'.format(element[0]))
                pass

        # split the paragraphs into sentences

        if len(words) > 0:
            re_us = re.compile(r'(U\.S\.).')  
            filtered_str = re.sub(re_us, 'USA ', words)

            re_blank = re.compile(
                r'(\\\\r\\\\n|(\s){2,})')  
            filtered_str = re.sub(re_blank, ' ', filtered_str)

            re_encode_issues = re.compile(r'(\\x.{2})')  
            filtered_str = re.sub(re_encode_issues, '', filtered_str)

            re_apos_issues = re.compile(r"(\\')")  
            filtered_str = re.sub(re_apos_issues, '', filtered_str)

            # next, split the file
            words = re.split(r'[\.?:][:"\\\\n\s]+', filtered_str)


        return [(element[0],
                 words)]  



def run():
    """
    Pipeline entry point, runs the all the necessary processes
    - read the html file from storage
    - parse file using bs4 and re
    - write output to .txt file
    """

    # instantiate PipelineOptions obj_ect using options dictionary
    pipeline_options = PipelineOptions()

    # Save main session state so pickled functions and classes
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # read the input,output and category file arguments as object
    runtime_options = PipelineOptions().view_as(RunTimeOptions)


    # set up the pipeline
    p = beam.Pipeline(options=pipeline_options)

    files_and_contents = (p
                          | 'ReadFilecontents' >> ReadFromTextWithFilename(runtime_options.input)
                          | 'aggregate' >> beam.GroupByKey())


    extract_process = (files_and_contents
                       | 'ProcesswithBSandSpacy' >> beam.ParDo(WordExtractingDoFn_Final()))

    # use the filter function to write only files where the parsing WordExtractingDoFn was successful
    write_process = (extract_process
                     | 'FilterforFilesWithContent' >> beam.Filter(lambda contents: len(contents[1]) > 0)
                     | 'ExtractOnlyWords' >> beam.Map(lambda contents: contents[1])
                     | 'FlattenSentenceList' >> beam.FlatMap(lambda contents: contents)
                     | 'WriteCompleteFiles' >> beam.io.WriteToText(runtime_options.output,
                                                                   append_trailing_newlines=True))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':  
    run()
