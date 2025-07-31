%% load dataprotocols.org Tabular Data Package into MATLAB table (or dataset)
%
%   [data, meta] = DATAPACKAGE(uri) returns a table(s) that are contained in
%   the datapackage formatted files contained in the directory or HTTP uri.
%   A struct with the contents of the `datapackage.json` file is returned as
%   meta.
%
%   [data, meta] = DATAPACKAGE(uri, ...) passes the arguments ... to the 
%   `readtable` or `dataset` reading functions (e.g. 'headerlines', 'format', 
%   'ReadVariableNames', etc.)
%
%   If no optional functions are passed in, defaults for the `readtable` and 
%   `dataset` functions are used.
%
%   If there are more than one resource file in the datapackage, pass in
%   optional arguments as cell strings or arrays. For example:
%       ..., 'format', {'%f%q%f', '%f%f'}
%       ..., readvarnames, [false, true]
%
%   The cell string/array of optional arguments must be the same length as the 
%   number of resources to be read in. That is, if you specify optional
%   parameters for one resource, you must specify that parameter for all 
%   resources.
%
%   Examples:
%       Load a datapackage from the web:
%           % Note the trailing '/' is important!
%           [data, meta] = DATAPACKAGE('http://data.okfn.org/data/core/gdp/');
%
%       Load the same datapackage from a local directory:
%           % trailing '\' is here too!
%           [data, meta] = DATAPACKAGE('C:\path\to\package\')
%
%   Troubleshooting:
%       A common error is failure to read a numeric field (column) because of
%       non-numeric characters in the field. The error message will look
%       something like "Unable to read the entire file.  You may need to 
%       specify a different format string, delimiter, or number of header
%       lines." Further, there at the bottom of the error message there will be
%       a "Caused by:" messaged with "Reading failed at line 170." 
%       
%       To fix this read error, specify a 'format' name/value pair to the 
%       DATAPACAKGE function. The format '%f' is for a numeric (floating point)
%       field and use '%q' for a text field ('q' makes the textscan function
%       keep double quoted values together. If you are having trouble, read in
%       everything as a text field ('%q').
%
%   See Also: README.md loadjson
%
%   LICENSE: (BSD-2, "FreeBSD" License)
%     Copyright (C) 2014, Kristofer D. Kusano (kdkusano@gmail.com)
%     All rights reserved
%
%     Redistribution and use in source and binary forms, with or without
%     modification, are permitted provided that the following conditions are met:
%     
%     1. Redistributions of source code must retain the above copyright notice, this
%        list of conditions and the following disclaimer. 
%     2. Redistributions in binary form must reproduce the above copyright notice,
%        this list of conditions and the following disclaimer in the documentation
%        and/or other materials provided with the distribution.
%     
%     THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
%     ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
%     WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%     DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
%     ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
%     (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
%     LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
%     ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%     (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%     SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

function [data, meta] = datapackage(uri, varargin)
%% Load data package and meta data from package

% depends on jsonlab library
mpath = fileparts(mfilename('fullpath'));
jsonpath = fullfile(mpath, 'jsonlab');
if exist(jsonpath, 'file')
    addpath(fullfile(mpath, 'jsonlab'));
else
    % we are possibly in release version - check for existance of function
    if exist('loadjson') ~= 2
        % 'loadjson' does not exists as a function
        error('datapackage:jsonlabNotFound',...
            'jsonlab function ''loadjson'' not found...');
    end
end

% use table by default, fall back on dataset (statistics toolbox)
v = ver('MATLAB');
v = v.Version;
v = regexp(v, '\.', 'split');
v = cellfun(@str2double, v);
if v(1) >= 8 && v(2) >= 2
    readfunc = 'table';
elseif license('test', 'Statistics_Toolbox')
    readfunc = 'dataset';
else
    error('datapackage:NoDataTables',...
        ['Your version of MATLAB has neither ''readtable'' nor ''dataset'' ',...
         'functions. Upgrade to at least R2013b or get a license for the ',...
         'Statistics Toolbox'])
end
    
% extract meta data from descriptor file
meta = open_descriptor(uri);

% read resources
data = get_resources(uri, meta, readfunc, varargin{:});

function s = open_resource(path)
%% read a resource to a string from either a URL or local file
try
    s = urlread(path);
catch me
    if strcmp(me.identifier, 'MATLAB:urlread:InvalidUrl')
        try
            % try as a local file
            if exist(path, 'file')
                fid = fopen(path, 'r');
                s = fscanf(fid, '%c');
                fclose(fid);
            else
                error('datapackage:LocalFileDoesNotExist',...
                    'file %s is not on the MATLAB path', path)
            end
        catch err
            rethrow(err)
        end
    else
        rethrow(me)
    end
end

function meta = open_descriptor(uri)
%% open the descriptor for the datapackage
descriptor_string = open_resource([uri, 'datapackage.json']);
meta = loadjson(descriptor_string);


function data = get_resources(uri, meta, readfunc, varargin)
%% open all resources as tables

% parse input
p = inputParser;
p.CaseSensitive = false;  % default settings
p.PartialMatching = true;
addRequired(p, 'uri', @ischar);  % required args
addRequired(p, 'meta');
addRequired(p, 'readfunc', @ischar);
ischar_or_cellstr = @(x) ischar(x) || iscellstr(x);
addParameter(p, 'treatasempty', '', ischar_or_cellstr);  % optional args
addParameter(p, 'delimiter', ',', ischar_or_cellstr);
addParameter(p, 'headerlines', 1, @isnumeric);
addParameter(p, 'readvarnames', false, @islogical);
addParameter(p, 'format', '', ischar_or_cellstr);
parse(p, uri, meta, readfunc, varargin{:});  % do parse

treatasempty = p.Results.treatasempty;
delimiter = p.Results.delimiter;
headerlines = p.Results.headerlines;
readvarnames = p.Results.readvarnames;
format_str_input = p.Results.format;

% TODO: check for "primaryKey" in fields hash

data = [];
if isfield(meta, 'resources') && ~isempty(meta.resources) 
    nr = length(meta.resources);
    data = cell(nr, 1);
    
    if ~iscell(meta.resources)
        mr = {meta.resources};
    else
        mr = meta.resources;
    end
    
    % set up multiple read options
    if nr > 1
        % if using defaults, repeat
        usedef = @(x) any(strcmp(p.UsingDefaults, x));
        if usedef('treatasempty')
            treatasempty = repmat({treatasempty}, nr, 1);
        else
            assert(length(treatasempty) == nr,...
                '''treatasempty'' option must be same length as resources');
        end
        if usedef('delimiter')
            delimiter = repmat({delimiter}, nr, 1);
        else
            assert(length(delimiter) == nr,...
                '''delimiter'' option must be same length as resources');
        end
        if usedef('headerlines')
            headerlines = repmat(headerlines, nr, 1);
        else
            assert(length(headerlines) == nr,...
                '''headerlines'' option must be same length as resources');
        end
        if usedef('readvarnames')
            readvarnames = repmat(readvarnames, nr, 1);
        else
            assert(length(readvarnames) == nr,...
                '''readvarnames'' option must be same length as resources');
        end
        if usedef('format')
            format_str_input = repmat({format_str_input}, nr, 1);
        else
            assert(length(format) == nr,...
                '''format'' option must be same length as resources');
        end
    end
    
    % import each resource
    for i = 1:nr
        % get resource, name
        r = mr{i};
        if isfield(r, 'name')
            rname = r.name;
        else
            rname = 'UNKNOWN';
        end
        
        % resource must have at least one
        if ~isfield(r, 'data') && ...
                ~isfield(r, 'path') && ...
                ~isfield(r, 'url')
            error('datapackage:ResourceReqFields',...
                ['Resource number %d does not have a field ''data'', '...
                 '''path'', or ''url'''], i);
        end
        
        % where is the data located?
        cleanup_temp = false;
        if isfield(r, 'data')
            % inline data
            if isfield(r, 'format')
                % what format is the inline data in?
                if strcmpi(r.format, 'json')
                    inline_format = 'json';
                elseif strcmpi(r.format, 'csv')
                    inline_format = 'csv';
                else
                    inline_format = '';
                end
            end
            
            % TODO: read inline data
            error('datapackage:InLineData',...
                'resource ''%s'' has inline data...need to add to program',...
                rname) 
        else
            % external resource
            if isfield(r, 'path') && exist(fullfile(uri, r.path), 'file')
                % check for local file
                resource_path = fullfile(uri, r.path);
            elseif isfield(r, 'url')
                % has path, but file not found - get from URL
                s = urlread(r.url);
                resource_path = [tempname, '.csv'];
                cleanup_temp = true;
                fid = fopen(resource_path, 'w');  % save as temp file
                fprintf(fid, '%s', s);
                fclose(fid);
            else
                % TODO: check for uri/r.path combo available on web
                error('datapackage:ResourcePathNotFound',...
                    ['Either local path does not exist or no url for ',...
                     'resource %d'],...
                    i);
            end
        end
        
        % arguments for read functions
        if iscellstr(delimiter)
            rdelimiter = delimiter{i};
        else
            rdelimiter = delimiter;
        end
        
        if iscellstr(format_str_input)
            format_str = format_str_input{i};
        else
            format_str = format_str_input;
        end
        
        if length(headerlines) > 1
            rheaderlines = headerlines(i);
        else
            rheaderlines = headerlines;
        end
        
        if iscell(treatasempty) && ~iscellstr(treatasempty)
            rtreatasempty = treatasempty{i};
        else
            rtreatasempty = treatasempty{i};    % JBG modified to avoid error
            %rtreatasempty = treatasempty;      % original version
        end
        
        if length(readvarnames) > 1
            rreadvarnames = readvarnames(i);
        else
            rreadvarnames = readvarnames;
        end
        
        % import schema, look for names/formats
        vnames = [];
        fieldtype = [];
        fieldformat = [];
        if isfield(r, 'schema')
            s = r.schema;
            if isfield(s, 'fields')
                f = s.fields;
                
                % iterate over all fields
                nf = length(f);
                vnames = cell(nf, 1);
                fieldtype = cell(nf, 1);
                fieldformat = cell(nf, 1);
                for j = 1:nf
                    if isfield(f{j}, 'name')
                        vnames{j} = f{j}.name;
                    end
                    if isfield(f{j}, 'type')
                        fieldtype{j} = f{j}.type;
                    end
                    if isfield(f{j}, 'format')
                        fieldformat{j} = f{j}.format;
                    end
                end
            end
        end
        
        % parse variable names
        if ~isempty(vnames)
            vempty = cellfun(@isempty, vnames);
            vempty_x = strcat('x', cellstr(num2str((1:sum(vempty))')));
            vnames(vempty) = vempty_x;
            vnames = cellfun(@genvarname, vnames, 'uni', false);
        end
        % check against first line
        fid = fopen(resource_path, 'r');
        raw = textscan(fid, '%s', 1,...
                         'headerlines', rheaderlines,...
                         'whitespace', '\n');
        fclose(fid);
        nvars_line1 = length(find(raw{1}{1} == rdelimiter)) + 1;
        
        nvars = nvars_line1;
        if ~isempty(vnames) && length(vnames) ~= nvars_line1
            nvars = max(nvars_line1, length(vnames));
            warning('datapackage:NVarsInSchemaDoNotMatch',...
                    ['Number of fields in schema (%d) does not match\n',...
                     'number of fields in 1st row of data file (%d)\n',...
                     'for resource %d. Using schema fields.\n',...
                     'You might need to specify a ''format'' option if\n',...
                     'there is a failure'],...
                    length(vnames), nvars, i);
        end
        
        % parse variable formats
        isdateformat = false(nvars, 1);
        isdatetimeformat = false(nvars, 1);
        if isempty(format_str) && ~isempty(fieldtype)
            format_str = repmat('%f', 1, nvars);
            for j = 1:nvars
                if strcmpi(fieldtype{j}, 'string')
                    format_str(2*j) = 'q';  % read, preserve double quote
                elseif strcmpi(fieldtype{j}, 'date')
                    format_str(2*j) = 'q';
                    isdateformat(j) = true;
                elseif strcmpi(fieldtype{j}, 'datetime')
                    format_str(2*j) = 'q';
                    isdatetimeformat(j) = true;
                elseif strcmpi(fieldtype{j}, 'object')
                    format_str(2*j) = 'q';
                    warning('''object'' format in resource ''%s''. Converting to string',...
                        r.name);
                elseif strcmpi(fieldtype{j}, 'geopoint') || strcmpi(fieldtype{j}, 'geojson')
                    format_str(2*j) = 'q';
                    warning('''geopoint'' format in resource ''%s''. Converting to string',...
                        r.name);
                elseif strcmpi(fieldtype{j}, 'array')
                    format_str(2*j) = 'q';
                    warning('''array'' format in resource ''%s''. Converting to string',...
                        r.name)
                end
            end
        end
        
        read_args = {
                     'delimiter', rdelimiter,...
                     'headerlines', rheaderlines,...
                     'treatasempty', rtreatasempty...
                     }; % common arguments
        if ~isempty(format_str)
            read_args = [read_args, {'format', format_str}];
        end
        
        % if no schema, read headers
        if isempty(vnames)
            warning(['No variable names found in resource schema %d. '...
                     'Attempting to read variable names from column heads'],...
                    i);
            rreadvarnames = true;
            iheaderlines = find(strcmpi(read_args, 'headerlines'));
            read_args{iheaderlines+1} = 0;
        end
        
        % read the data
        if strcmp(readfunc, 'table')
            data{i} = readtable(resource_path,...
                                'FileType', 'text',...
                                'ReadVariableNames', rreadvarnames,...
                                read_args{:} ...
                                );
            if ~isempty(vnames)
                data{i}.Properties.VariableNames = vnames;
            else
                vnames = data{i}.Properties.VariableNames;
            end
        elseif strcmp(readfunc, 'dataset')
            data{i} = dataset('File', resource_path,...
                              'readvarnames', rreadvarnames,...
                              'VarNames', vnames,...
                              read_args{:} ...
                              );
            if ~isempty(vnames)
                data{i}.Properties.VarNames = vnames;
            else
                vnames = data{i}.Properties.VarNames;
            end
        else
            error('datapackage:Invalidreadfunc',...
                'Invalid ''readfunc'' value');
        end
        
        % clean up
        if cleanup_temp
            delete(resource_path);
        end
        
        % convert date formats
        idate = find(isdateformat | isdatetimeformat);
        for j = 1:length(idate)
            try
                jdate = data{i}.(vnames{idate(j)});
                if ~isempty(fieldformat{idate(j)})
                    % use format, if it exists
                    jdateformat = fieldformat{idate(j)};
                    date_number = datenum(jdate, jdateformat);
                else
                    % no format, just try it
                    date_number = datenum(jdate);
                end
                data{i}.(vnames{idate(j)}) = date_number;
            catch
                % TODO: make a way for user to input date format
                warning(['Failed to parse field ''%s'' as a date string ',...
                         'in resource %d. Keeping it as a string'],...
                        vnames{j}, i);
            end
        end
    end
    
    % expand if only 1 resource
    if nr == 1
        data = data{1};
    end
end

% there MUST be new line(s) at the end of the file
% `make` appends `loadjson.m` here
% ---------------------------- END datapackage.m ---------------------------- %
function data = loadjson(fname,varargin)
%
% data=loadjson(fname,opt)
%    or
% data=loadjson(fname,'param1',value1,'param2',value2,...)
%
% parse a JSON (JavaScript Object Notation) file or string
%
% authors:Qianqian Fang (fangq<at> nmr.mgh.harvard.edu)
%            date: 2011/09/09
%         Nedialko Krouchev: http://www.mathworks.com/matlabcentral/fileexchange/25713
%            date: 2009/11/02
%         François Glineur: http://www.mathworks.com/matlabcentral/fileexchange/23393
%            date: 2009/03/22
%         Joel Feenstra:
%         http://www.mathworks.com/matlabcentral/fileexchange/20565
%            date: 2008/07/03
%
% $Id$
%
% input:
%      fname: input file name, if fname contains "{}" or "[]", fname
%             will be interpreted as a JSON string
%      opt: a struct to store parsing options, opt can be replaced by 
%           a list of ('param',value) pairs. The param string is equivallent
%           to a field in opt.
%
% output:
%      dat: a cell array, where {...} blocks are converted into cell arrays,
%           and [...] are converted to arrays
%
% license:
%     BSD or GPL version 3, see LICENSE_{BSD,GPLv3}.txt files for details 
%
% -- this function is part of jsonlab toolbox (http://iso2mesh.sf.net/cgi-bin/index.cgi?jsonlab)
%

global pos inStr len  esc index_esc len_esc isoct arraytoken

if(regexp(fname,'[\{\}\]\[]','once'))
   string=fname;
elseif(exist(fname,'file'))
   fid = fopen(fname,'rb');
   string = fread(fid,inf,'uint8=>char')';
   fclose(fid);
else
   error('input file does not exist');
end

pos = 1; len = length(string); inStr = string;
isoct=exist('OCTAVE_VERSION','builtin');
arraytoken=find(inStr=='[' | inStr==']' | inStr=='"');
jstr=regexprep(inStr,'\\\\','  ');
escquote=regexp(jstr,'\\"');
arraytoken=sort([arraytoken escquote]);

% String delimiters and escape chars identified to improve speed:
esc = find(inStr=='"' | inStr=='\' ); % comparable to: regexp(inStr, '["\\]');
index_esc = 1; len_esc = length(esc);

opt=varargin2struct(varargin{:});
jsoncount=1;
while pos <= len
    switch(next_char)
        case '{'
            data{jsoncount} = parse_object(opt);
        case '['
            data{jsoncount} = parse_array(opt);
        otherwise
            error_pos('Outer level structure must be an object or an array');
    end
    jsoncount=jsoncount+1;
end % while

jsoncount=length(data);
if(jsoncount==1 && iscell(data))
    data=data{1};
end

if(~isempty(data))
      if(isstruct(data)) % data can be a struct array
          data=jstruct2array(data);
      elseif(iscell(data))
          data=jcell2array(data);
      end
end


%%
function newdata=jcell2array(data)
len=length(data);
newdata=data;
for i=1:len
      if(isstruct(data{i}))
          newdata{i}=jstruct2array(data{i});
      elseif(iscell(data{i}))
          newdata{i}=jcell2array(data{i});
      end
end

%%-------------------------------------------------------------------------
function newdata=jstruct2array(data)
fn=fieldnames(data);
newdata=data;
len=length(data);
for i=1:length(fn) % depth-first
    for j=1:len
        if(isstruct(getfield(data(j),fn{i})))
            newdata(j)=setfield(newdata(j),fn{i},jstruct2array(getfield(data(j),fn{i})));
        end
    end
end
if(~isempty(strmatch('x0x5F_ArrayType_',fn)) && ~isempty(strmatch('x0x5F_ArrayData_',fn)))
  newdata=cell(len,1);
  for j=1:len
    ndata=cast(data(j).x0x5F_ArrayData_,data(j).x0x5F_ArrayType_);
    iscpx=0;
    if(~isempty(strmatch('x0x5F_ArrayIsComplex_',fn)))
        if(data(j).x0x5F_ArrayIsComplex_)
           iscpx=1;
        end
    end
    if(~isempty(strmatch('x0x5F_ArrayIsSparse_',fn)))
        if(data(j).x0x5F_ArrayIsSparse_)
            if(~isempty(strmatch('x0x5F_ArraySize_',fn)))
                dim=data(j).x0x5F_ArraySize_;
                if(iscpx && size(ndata,2)==4-any(dim==1))
                    ndata(:,end-1)=complex(ndata(:,end-1),ndata(:,end));
                end
                if isempty(ndata)
                    % All-zeros sparse
                    ndata=sparse(dim(1),prod(dim(2:end)));
                elseif dim(1)==1
                    % Sparse row vector
                    ndata=sparse(1,ndata(:,1),ndata(:,2),dim(1),prod(dim(2:end)));
                elseif dim(2)==1
                    % Sparse column vector
                    ndata=sparse(ndata(:,1),1,ndata(:,2),dim(1),prod(dim(2:end)));
                else
                    % Generic sparse array.
                    ndata=sparse(ndata(:,1),ndata(:,2),ndata(:,3),dim(1),prod(dim(2:end)));
                end
            else
                if(iscpx && size(ndata,2)==4)
                    ndata(:,3)=complex(ndata(:,3),ndata(:,4));
                end
                ndata=sparse(ndata(:,1),ndata(:,2),ndata(:,3));
            end
        end
    elseif(~isempty(strmatch('x0x5F_ArraySize_',fn)))
        if(iscpx && size(ndata,2)==2)
             ndata=complex(ndata(:,1),ndata(:,2));
        end
        ndata=reshape(ndata(:),data(j).x0x5F_ArraySize_);
    end
    newdata{j}=ndata;
  end
  if(len==1)
      newdata=newdata{1};
  end
end

%%-------------------------------------------------------------------------
function object = parse_object(varargin)
    parse_char('{');
    object = [];
    if next_char ~= '}'
        while 1
            str = parseStr(varargin{:});
            if isempty(str)
                error_pos('Name of value at position %d cannot be empty');
            end
            parse_char(':');
            val = parse_value(varargin{:});
            eval( sprintf( 'object.%s  = val;', valid_field(str) ) );
            if next_char == '}'
                break;
            end
            parse_char(',');
        end
    end
    parse_char('}');

%%-------------------------------------------------------------------------

function object = parse_array(varargin) % JSON array is written in row-major order
global pos inStr isoct
    parse_char('[');
    object = cell(0, 1);
    dim2=[];
    if next_char ~= ']'
        [endpos e1l e1r maxlevel]=matching_bracket(inStr,pos);
        arraystr=['[' inStr(pos:endpos)];
        arraystr=regexprep(arraystr,'"_NaN_"','NaN');
        arraystr=regexprep(arraystr,'"([-+]*)_Inf_"','$1Inf');
        arraystr(find(arraystr==sprintf('\n')))=[];
        arraystr(find(arraystr==sprintf('\r')))=[];
        %arraystr=regexprep(arraystr,'\s*,',','); % this is slow,sometimes needed
        if(~isempty(e1l) && ~isempty(e1r)) % the array is in 2D or higher D
            astr=inStr((e1l+1):(e1r-1));
            astr=regexprep(astr,'"_NaN_"','NaN');
            astr=regexprep(astr,'"([-+]*)_Inf_"','$1Inf');
            astr(find(astr==sprintf('\n')))=[];
            astr(find(astr==sprintf('\r')))=[];
            astr(find(astr==' '))='';
            if(isempty(find(astr=='[', 1))) % array is 2D
                dim2=length(sscanf(astr,'%f,',[1 inf]));
            end
        else % array is 1D
            astr=arraystr(2:end-1);
            astr(find(astr==' '))='';
            [obj count errmsg nextidx]=sscanf(astr,'%f,',[1,inf]);
            if(nextidx>=length(astr)-1)
                object=obj;
                pos=endpos;
                parse_char(']');
                return;
            end
        end
        if(~isempty(dim2))
            astr=arraystr;
            astr(find(astr=='['))='';
            astr(find(astr==']'))='';
            astr(find(astr==' '))='';
            [obj count errmsg nextidx]=sscanf(astr,'%f,',inf);
            if(nextidx>=length(astr)-1)
                object=reshape(obj,dim2,numel(obj)/dim2)';
                pos=endpos;
                parse_char(']');
                return;
            end
        end
        arraystr=regexprep(arraystr,'\]\s*,','];');
        try
           if(isoct && regexp(arraystr,'"','once'))
                error('Octave eval can produce empty cells for JSON-like input');
           end
           object=eval(arraystr);
           pos=endpos;
        catch
         while 1
            val = parse_value(varargin{:});
            object{end+1} = val;
            if next_char == ']'
                break;
            end
            parse_char(',');
         end
        end
    end
    if(jsonopt('SimplifyCell',0,varargin{:})==1)
      try
        oldobj=object;
        object=cell2mat(object')';
        if(iscell(oldobj) && isstruct(object) && numel(object)>1 && jsonopt('SimplifyCellArray',1,varargin{:})==0)
            object=oldobj;
        elseif(size(object,1)>1 && ndims(object)==2)
            object=object';
        end
      catch
      end
    end
    parse_char(']');

%%-------------------------------------------------------------------------

function parse_char(c)
    global pos inStr len
    skip_whitespace;
    if pos > len || inStr(pos) ~= c
        error_pos(sprintf('Expected %c at position %%d', c));
    else
        pos = pos + 1;
        skip_whitespace;
    end

%%-------------------------------------------------------------------------

function c = next_char
    global pos inStr len
    skip_whitespace;
    if pos > len
        c = [];
    else
        c = inStr(pos);
    end

%%-------------------------------------------------------------------------

function skip_whitespace
    global pos inStr len
    while pos <= len && isspace(inStr(pos))
        pos = pos + 1;
    end

%%-------------------------------------------------------------------------
function str = parseStr(varargin)
    global pos inStr len  esc index_esc len_esc
 % len, ns = length(inStr), keyboard
    if inStr(pos) ~= '"'
        error_pos('String starting with " expected at position %d');
    else
        pos = pos + 1;
    end
    str = '';
    while pos <= len
        while index_esc <= len_esc && esc(index_esc) < pos
            index_esc = index_esc + 1;
        end
        if index_esc > len_esc
            str = [str inStr(pos:len)];
            pos = len + 1;
            break;
        else
            str = [str inStr(pos:esc(index_esc)-1)];
            pos = esc(index_esc);
        end
        nstr = length(str); switch inStr(pos)
            case '"'
                pos = pos + 1;
                if(~isempty(str))
                    if(strcmp(str,'_Inf_'))
                        str=Inf;
                    elseif(strcmp(str,'-_Inf_'))
                        str=-Inf;
                    elseif(strcmp(str,'_NaN_'))
                        str=NaN;
                    end
                end
                return;
            case '\'
                if pos+1 > len
                    error_pos('End of file reached right after escape character');
                end
                pos = pos + 1;
                switch inStr(pos)
                    case {'"' '\' '/'}
                        str(nstr+1) = inStr(pos);
                        pos = pos + 1;
                    case {'b' 'f' 'n' 'r' 't'}
                        str(nstr+1) = sprintf(['\' inStr(pos)]);
                        pos = pos + 1;
                    case 'u'
                        if pos+4 > len
                            error_pos('End of file reached in escaped unicode character');
                        end
                        str(nstr+(1:6)) = inStr(pos-1:pos+4);
                        pos = pos + 5;
                end
            otherwise % should never happen
                str(nstr+1) = inStr(pos), keyboard
                pos = pos + 1;
        end
    end
    error_pos('End of file while expecting end of inStr');

%%-------------------------------------------------------------------------

function num = parse_number(varargin)
    global pos inStr len isoct
    currstr=inStr(pos:end);
    numstr=0;
    if(isoct~=0)
        numstr=regexp(currstr,'^\s*-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+\-]?\d+)?','end');
        [num, one] = sscanf(currstr, '%f', 1);
        delta=numstr+1;
    else
        [num, one, err, delta] = sscanf(currstr, '%f', 1);
        if ~isempty(err)
            error_pos('Error reading number at position %d');
        end
    end
    pos = pos + delta-1;

%%-------------------------------------------------------------------------

function val = parse_value(varargin)
    global pos inStr len
    true = 1; false = 0;

    switch(inStr(pos))
        case '"'
            val = parseStr(varargin{:});
            return;
        case '['
            val = parse_array(varargin{:});
            return;
        case '{'
            val = parse_object(varargin{:});
            if isstruct(val)
                if(~isempty(strmatch('x0x5F_ArrayType_',fieldnames(val), 'exact')))
                    val=jstruct2array(val);
                end
            elseif isempty(val)
                val = struct;
            end
            return;
        case {'-','0','1','2','3','4','5','6','7','8','9'}
            val = parse_number(varargin{:});
            return;
        case 't'
            if pos+3 <= len && strcmpi(inStr(pos:pos+3), 'true')
                val = true;
                pos = pos + 4;
                return;
            end
        case 'f'
            if pos+4 <= len && strcmpi(inStr(pos:pos+4), 'false')
                val = false;
                pos = pos + 5;
                return;
            end
        case 'n'
            if pos+3 <= len && strcmpi(inStr(pos:pos+3), 'null')
                val = [];
                pos = pos + 4;
                return;
            end
    end
    error_pos('Value expected at position %d');
%%-------------------------------------------------------------------------

function error_pos(msg)
    global pos inStr len
    poShow = max(min([pos-15 pos-1 pos pos+20],len),1);
    if poShow(3) == poShow(2)
        poShow(3:4) = poShow(2)+[0 -1];  % display nothing after
    end
    msg = [sprintf(msg, pos) ': ' ...
    inStr(poShow(1):poShow(2)) '<error>' inStr(poShow(3):poShow(4)) ];
    error( ['JSONparser:invalidFormat: ' msg] );

%%-------------------------------------------------------------------------

function str = valid_field(str)
global isoct
% From MATLAB doc: field names must begin with a letter, which may be
% followed by any combination of letters, digits, and underscores.
% Invalid characters will be converted to underscores, and the prefix
% "x0x[Hex code]_" will be added if the first character is not a letter.
    pos=regexp(str,'^[^A-Za-z]','once');
    if(~isempty(pos))
        if(~isoct)
            str=regexprep(str,'^([^A-Za-z])','x0x${sprintf(''%X'',unicode2native($1))}_','once');
        else
            str=sprintf('x0x%X_%s',char(str(1)),str(2:end));
        end
    end
    if(isempty(regexp(str,'[^0-9A-Za-z_]', 'once' ))) return;  end
    if(~isoct)
        str=regexprep(str,'([^0-9A-Za-z_])','_0x${sprintf(''%X'',unicode2native($1))}_');
    else
        pos=regexp(str,'[^0-9A-Za-z_]');
        if(isempty(pos)) return; end
        str0=str;
        pos0=[0 pos(:)' length(str)];
        str='';
        for i=1:length(pos)
            str=[str str0(pos0(i)+1:pos(i)-1) sprintf('_0x%X_',str0(pos(i)))];
        end
        if(pos(end)~=length(str))
            str=[str str0(pos0(end-1)+1:pos0(end))];
        end
    end
    %str(~isletter(str) & ~('0' <= str & str <= '9')) = '_';

%%-------------------------------------------------------------------------
function endpos = matching_quote(str,pos)
len=length(str);
while(pos<len)
    if(str(pos)=='"')
        if(~(pos>1 && str(pos-1)=='\'))
            endpos=pos;
            return;
        end        
    end
    pos=pos+1;
end
error('unmatched quotation mark');
%%-------------------------------------------------------------------------
function [endpos e1l e1r maxlevel] = matching_bracket(str,pos)
global arraytoken
level=1;
maxlevel=level;
endpos=0;
bpos=arraytoken(arraytoken>=pos);
tokens=str(bpos);
len=length(tokens);
pos=1;
e1l=[];
e1r=[];
while(pos<=len)
    c=tokens(pos);
    if(c==']')
        level=level-1;
        if(isempty(e1r)) e1r=bpos(pos); end
        if(level==0)
            endpos=bpos(pos);
            return
        end
    end
    if(c=='[')
        if(isempty(e1l)) e1l=bpos(pos); end
        level=level+1;
        maxlevel=max(maxlevel,level);
    end
    if(c=='"')
        pos=matching_quote(tokens,pos+1);
    end
    pos=pos+1;
end
if(endpos==0) 
    error('unmatched "]"');
end

function opt=varargin2struct(varargin)
%
% opt=varargin2struct('param1',value1,'param2',value2,...)
%   or
% opt=varargin2struct(...,optstruct,...)
%
% convert a series of input parameters into a structure
%
% authors:Qianqian Fang (fangq<at> nmr.mgh.harvard.edu)
% date: 2012/12/22
%
% input:
%      'param', value: the input parameters should be pairs of a string and a value
%       optstruct: if a parameter is a struct, the fields will be merged to the output struct
%
% output:
%      opt: a struct where opt.param1=value1, opt.param2=value2 ...
%
% license:
%     BSD or GPL version 3, see LICENSE_{BSD,GPLv3}.txt files for details 
%
% -- this function is part of jsonlab toolbox (http://iso2mesh.sf.net/cgi-bin/index.cgi?jsonlab)
%

len=length(varargin);
opt=struct;
if(len==0) return; end
i=1;
while(i<=len)
    if(isstruct(varargin{i}))
        opt=mergestruct(opt,varargin{i});
    elseif(ischar(varargin{i}) && i<len)
        opt=setfield(opt,varargin{i},varargin{i+1});
        i=i+1;
    else
        error('input must be in the form of ...,''name'',value,... pairs or structs');
    end
    i=i+1;
end

function val=jsonopt(key,default,varargin)
%
% val=jsonopt(key,default,optstruct)
%
% setting options based on a struct. The struct can be produced
% by varargin2struct from a list of 'param','value' pairs
%
% authors:Qianqian Fang (fangq<at> nmr.mgh.harvard.edu)
%
% $Id: loadjson.m 371 2012-06-20 12:43:06Z fangq $
%
% input:
%      key: a string with which one look up a value from a struct
%      default: if the key does not exist, return default
%      optstruct: a struct where each sub-field is a key 
%
% output:
%      val: if key exists, val=optstruct.key; otherwise val=default
%
% license:
%     BSD or GPL version 3, see LICENSE_{BSD,GPLv3}.txt files for details
%
% -- this function is part of jsonlab toolbox (http://iso2mesh.sf.net/cgi-bin/index.cgi?jsonlab)
% 

val=default;
if(nargin<=2) return; end
opt=varargin{1};
if(isstruct(opt) && isfield(opt,key))
    val=getfield(opt,key);
end

