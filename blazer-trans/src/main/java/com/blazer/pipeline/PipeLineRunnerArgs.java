package com.blazer.pipeline;

import com.beust.jcommander.Parameter;

public class PipeLineRunnerArgs {

    @Parameter(names = "--config", description = "Task Runner Application Context Configuration XML File , must in spring folder")
    public String config;
    
    @Parameter(names = "--sqlwhere", description = "Sql for Export")
    public String sqlWhere;
    
    @Parameter(names = "--etldate", description = "Cmd Call Date")
    public String etlDate;

}
