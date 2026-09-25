add_executable(dpvtk-ar dpvtk-ar.C)
target_link_libraries(dpvtk-ar PRIVATE DPvzSer)
install(TARGETS dpvtk-ar DESTINATION "${CMAKE_INSTALL_BINDIR}")
